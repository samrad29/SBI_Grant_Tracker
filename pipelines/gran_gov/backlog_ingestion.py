"""
This module contains functions to ingest the backlog of grants from the grant.gov API.
"""
import sqlite3
import time
from pipelines.gran_gov.main import get_opportunity_ids
from pipelines.gran_gov.ingestion_loop import upsert_grant_current, insert_snapshot
from pipelines.gran_gov.ingestion_utils import fetch_opportunity, normalize_opportunity, update_grant_classification
from pipelines.gran_gov.ai_utils import classify_grant, get_groq_client
from pipelines.gran_gov.init_tables import create_tables
from pipelines.gran_gov.quick_classification import quick_classification
from db.db_util import get_db_connection


def ingest_backlog(conn: sqlite3.Connection, test_mode: int = 0):
    """
    Ingests the backlog of grants from the grant.gov API.
    """
    quick_labelled_relevant = 0
    ai_labelled_relevant = 0
    ai_used = 0
    failed = 0

    #1: grab all opportunity ids from the database
    opportunity_ids = get_opportunity_ids(test_mode=test_mode)
    print(f"Found {len(opportunity_ids)} opportunity ids.")

    #2: check if the opportunity ID is already in the database
    existing_ids = {
        row[0] for row in conn.execute(
            "SELECT opportunity_id FROM grants"
        ).fetchall()
    }
    new_ids = [oid for oid in opportunity_ids if oid not in existing_ids]
    print(f"Found {len(new_ids)} new opportunity ids.")

    #3: Pull the details for each opportunity id and put into DB
    groq_client = get_groq_client()
    for i, opportunity_id in enumerate(new_ids):
        try:
            print("--------------------------------")
            raw = fetch_opportunity(opportunity_id)
            print(f"Pulled details for {opportunity_id}")
            normalized = normalize_opportunity(raw)
            normalized["id"] = str(opportunity_id)
            print("Normalized data")
            print(f"Title: {normalized['title']}")
            print(f"ShortDescription: {(normalized.get('description') or '')[:100]}")

            #4: Upsert the grant and the snapshot to the database
            upsert_grant_current(conn, normalized)
            insert_snapshot(conn, str(opportunity_id), normalized)
            print("Upserted grant and snapshot completed")

            #4: Classify the grant
            quick_check_result = quick_classification(normalized)
            print(f"Quick classification result: {quick_check_result}")
            if quick_check_result["is_relevant"]:
                update_grant_classification(conn, opportunity_id, quick_check_result)
                quick_labelled_relevant += 1
                continue
            
            #5: If the grant is not relevant, send it to AI for further classification
            if quick_check_result["needs_ai"]:
                ai_used +=1
                print("Sending to AI for further classification")
                ai_result = classify_grant(groq_client, normalized)
                if ai_result is None:
                    print("AI classification failed (returned None); skipping.")
                else:
                    update_grant_classification(conn, opportunity_id, ai_result)
                    print(f"AI classification result: {ai_result}")
                    if ai_result.get("is_relevant"):
                        ai_labelled_relevant += 1
            if i % 50 == 0: 
                conn.commit()
            if i % 10 == 0: 
                time.sleep(0.5)

        except Exception as e:
            print(f"Error processing grant {opportunity_id}: {e}")
            failed += 1
            continue

    print("--------------------------------")
    print("Ingestion results:")
    print(f"Total new grants ingested: {len(new_ids)}")
    print(f"Quick labelled relevant: {quick_labelled_relevant}")
    print(f"AI used to classify: {ai_used}")
    print(f"AI labelled relevant: {ai_labelled_relevant}")
    print(f"Failed to ingest: {failed}")
    print("--------------------------------")

if __name__ == "__main__":
    start_time = time.time()
    start_time_str = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"Starting backlog ingestion at {start_time_str}")
    print("--------------------------------")
    print("Connecting to database...")
    conn = get_db_connection()
    print("Creating tables...")
    create_tables(conn)
    print("Ingesting backlog...")
    ingest_backlog(conn, test_mode=0)
    print("Committing changes...")
    conn.commit()
    print("Closing database connection...")
    conn.close()
    print("--------------------------------")
    print(f"Backlog ingestion completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Start time: {start_time_str}")
    print(f"Total time taken: {time.time() - start_time} seconds")
    print("--------------------------------")