import requests
from pipelines.gran_gov.init_tables import create_tables
from pipelines.gran_gov.ingestion_loop import daily_ingestion
from pipelines.gran_gov.ingestion_utils import trim_opportunity_ids, update_last_seen_at, archive_old_grants
from jobs.log_utils import log
from db.db_util import is_test_mode
import os

SEARCH_URL = "https://api.grants.gov/v1/api/search2"

def get_opportunity_ids(keywords: list[str] = [""], eligibilities: str = "",test_mode: int = 0) -> list[int]:
    """
    Function for getting a list of opportunity ids from the grant.gov API.
    You can define different search parameters for the API call.
    """
    print(f"Getting opportunity ids for keywords: {keywords} and eligibilities: {eligibilities}")
    # If test_mode is greater than 0, set rows to 5
    if test_mode > 0:
        rows = 5
    else:
        rows = 2000

    opportunity_ids = []
    for keyword in keywords:
        payload = {
            "keyword": keyword,
            "rows": rows,
            "oppStatuses": "forecasted|posted",
            "eligibilities": eligibilities,
            "fundingCategories": "",
        }
        print(f"Search payload: {payload}")

        headers = {"Content-Type": "application/json"}

        response = requests.post(SEARCH_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        results = response.json()
        if results.get("errorcode", -1) != 0:
            raise RuntimeError(f"API error: {results.get('msg', 'Unknown error')}")
        data = results.get("data", {})
        opportunity_ids.extend([hit.get("id") for hit in data.get("oppHits", [])])
    return opportunity_ids

def grants_main(conn, job_id: int, daily: bool = True) -> None:
    print("Starting grant ingestion loop...")
    if daily: 
        print("Starting daily grant ingestion loop...")
        log(conn, job_id, "Starting daily grant ingestion loop...", "INFO")
    else:
        print("Starting weekly grant ingestion loop...")
        log(conn, job_id, "Starting weekly grant ingestion loop...", "INFO")
    create_tables(conn)
    if os.getenv("TEST_5_IDS") == "False":
        test_mode = 0
    else:
        test_mode = 1

    #1: get all active/forecasted opportunity ids from the API
    opportunity_ids = get_opportunity_ids(test_mode=test_mode)
    log(conn, job_id, f"Found {len(opportunity_ids)} opportunity ids.", "INFO")
    print(f"Found {len(opportunity_ids)} opportunity ids.")

    #2: update the last_seen_at column for all opportunity ids
    update_last_seen_at(opportunity_ids, conn, job_id)
    log(conn, job_id, f"Updated the last_seen_at column for {len(opportunity_ids)} opportunity ids.", "INFO")
    print(f"Updated the last_seen_at column for {len(opportunity_ids)} opportunity ids.")

    if daily:
        #3: for daily ingestion,remove old and irrelevant opportunity ids from the list
        opportunity_ids = trim_opportunity_ids(opportunity_ids, conn)
        log(conn, job_id, f"After trimming, {len(opportunity_ids)} opportunity ids remain.", "INFO")
        print(f"After trimming, {len(opportunity_ids)} opportunity ids remain.")
    else: 
        log(conn, job_id, "No trimming of opportunity ids needed for weekly ingestion.", "INFO")
    #4: investigate new and relevant grants
    if opportunity_ids:
        daily_ingestion(conn, opportunity_ids, job_id)
        log(conn, job_id, "Grant ingestion loop completed successfully.", "INFO")
        print("Grant ingestion loop completed successfully.")
    else:
        log(conn, job_id, "No opportunity ids found. Exiting...", "ERROR")
        print("No opportunity ids found. Exiting...")

    #5: archive grants that have not been seen in the past 5 days
    archived_grants = archive_old_grants(conn, job_id)
    log(conn, job_id, f"Archived {archived_grants} grants that have not been seen in the past 5 days.", "INFO")
    print(f"Archived {archived_grants} grants that have not been seen in the past 5 days.")
    return
