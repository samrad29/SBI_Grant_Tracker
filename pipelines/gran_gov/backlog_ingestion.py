"""
This module contains functions to ingest the backlog of grants from the grant.gov API.
"""
import sqlite3
import time
from pipelines.gran_gov.main import get_opportunity_ids
from pipelines.gran_gov.ingestion_loop import upsert_grant_current, insert_snapshot
from pipelines.gran_gov.ingestion_utils import fetch_opportunity, normalize_opportunity, update_tribal_eligibility, update_grant_tags
from pipelines.gran_gov.ai_utils import ai_grant_tagging, ai_tribal_eligibility_check, get_llm_client, RateLimitError
from pipelines.gran_gov.init_tables import create_tables
from pipelines.gran_gov.quick_classification import quick_classification
from jobs.log_utils import log
from db.db_util import get_db_connection, is_test_mode
from jobs.log_utils import create_pipeline_run


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

    #2: check if the opportunity ID is already in the database and has tags
    existing_ids = {
        row[0] for row in conn.execute(
            "SELECT distinct opportunity_id FROM grant_tags"
        ).fetchall()
    }
    new_ids = [oid for oid in opportunity_ids if oid not in existing_ids]
    print(f"Found {len(new_ids)} untagged opportunity ids.")

    #3: Pull the details for each opportunity id and put into DB
    llm_client = get_llm_client()
    i = 0
    max_failures = 25
    retry_count = 0
    batch_size = 300
    while i < len(new_ids) and i < batch_size:
        opportunity_id = new_ids[i]
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

            #4: Classify the grant as tribal eligible
            quick_check_result = quick_classification(normalized)
            print(f"Quick classification result: {quick_check_result}")
            if quick_check_result["is_tribal_eligible"]:
                update_tribal_eligibility(conn, opportunity_id, quick_check_result)
                quick_labelled_relevant += 1
            
            #5: If the grant is not relevant, send it to AI for further classification
            if quick_check_result["needs_ai"]:
                ai_used +=1
                print("Sending to AI for further classification")
                ai_result = ai_tribal_eligibility_check(llm_client, normalized)
                if ai_result is None:
                    print("AI classification failed (returned None); skipping.")
                else:
                    update_tribal_eligibility(conn, opportunity_id, ai_result)
                    print(f"AI classification result: {ai_result}")
                    if ai_result.get("is_tribal_eligible"):
                        ai_labelled_relevant += 1
            #6: Tag the grant
            ai_result = ai_grant_tagging(llm_client, normalized)
            if ai_result is None:
                print("AI tagging failed (returned None); skipping.")
                failed += 1
            else:
                update_grant_tags(conn, opportunity_id, ai_result, -1) # job_id -1 means no job_id (for backlog ingestion)
                print(f"AI tagging result: {ai_result}")

            if i % 50 == 0: 
                conn.commit()
            if i % 10 == 0: 
                time.sleep(0.5)
            i += 1
            retry_count = 0
            if failed > max_failures:
                print(f"Failed to ingest {failed} grants after {max_failures} failures. Stopping ingestion...")
                break

        except RateLimitError as e:
            pause_s = float(getattr(e, "retry_seconds", 10.0) or 10.0)
            print(f"Hit AI 429 rate limit. Pausing ingestion for {pause_s:.1f}s and retrying...")
            if retry_count < 3:
                retry_count += 1
                time.sleep(pause_s)
                continue
            else:
                print(f"Failed to ingest grant {opportunity_id} after 3 retries. Skipping...")
                failed += 1
                if failed > max_failures:
                    print(f"Failed to ingest {failed} grants after {max_failures} failures. Stopping ingestion...")
                    break
                else:
                    i += 1
                    continue
        except Exception as e:
            print(f"Error processing grant {opportunity_id}: {e}")
            failed += 1
            if failed > max_failures:
                print(f"Failed to ingest {failed} grants after {max_failures} failures. Stopping ingestion...")
                break
            i += 1
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
    test_mode = 1 if is_test_mode() else 0
    conn = get_db_connection(test_mode=bool(test_mode))
    print("Creating tables...")
    create_tables(conn)
    print("Ingesting backlog...")
    ingest_backlog(conn, test_mode=test_mode)
    print("Committing changes...")
    conn.commit()
    print("Closing database connection...")
    conn.close()
    print("--------------------------------")
    print(f"Backlog ingestion completed at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Start time: {start_time_str}")
    print(f"Total time taken: {time.time() - start_time} seconds")
    print("--------------------------------")