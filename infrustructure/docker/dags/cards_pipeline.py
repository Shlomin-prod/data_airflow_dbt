"""
Daily/hourly ingestion DAG:
  1. Page through the strictlybetter.eu "obsoletes" API and dump to JSON.
  2. Upload that JSON into a Databricks Unity Catalog volume.
  3. Trigger a downstream Databricks job to process it.
 
Fixes applied vs. the original version:
  - File path is generated ONCE (inside the extract task) and passed to
    downstream tasks via XCom / TaskFlow return values, instead of being
    recomputed with datetime.now() at DAG-parse time and again at
    task-run time (those two timestamps almost never matched, which
    caused FileNotFoundError on the upload step).
  - Pagination now actually respects `last_page` instead of the
    hardcoded `page >= 5` debug limit (kept as an optional safety cap).
  - fetch_from_api raises on failure instead of referencing an
    undefined `data` variable.
  - Filenames are based on the DAG's logical date (`ds`), not
    datetime.now(), so re-runs/backfills are idempotent and don't
    create a new file every time.
  - HTTP calls use a requests.Session with retry/backoff.
  - Output directory is created if missing.
  - dag_id/schedule naming inconsistency flagged (see NOTE below).
  - Added default_args (retries, retry_delay), tags, doc_md.
"""
 
from __future__ import annotations
 
import io
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
 
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
 
from airflow.decorators import dag, task
from databricks.sdk import WorkspaceClient

 
CATALOG = "palkin"
SCHEMA = "weather"
VOLUME = "weather_bronze"
 
DATA_DIR = Path("/opt/airflow/data")
API_BASE_URL = "https://www.strictlybetter.eu/api/obsoletes"
DATABRICKS_JOB_ID = 1077892079944136
 
# Safety cap so a bug/misbehaving API can't page forever. Set to None to
# always page through everything the API reports via `last_page`.
MAX_PAGES = 5
 
 
def _requests_session() -> requests.Session:
    """A session with sane retry/backoff for transient failures."""
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session
 
 
def _fetch_page(session: requests.Session, page: int) -> dict:
    response = session.get(API_BASE_URL, params={"page": page}, timeout=30)
    
    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch page {page} from API. "
            f"Status code: {response.status_code}, body: {response.text[:500]}"
        )
    return response.json()
 
 
def _databricks_client() -> WorkspaceClient:
    return WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
 
 
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}
 
 
@dag(
    dag_id="cards_pipeline",
    # NOTE: dag_id says "daily" but the original schedule was "@hourly".
    # Pick whichever is actually intended -- left as @hourly since that's
    # what was running; rename the dag_id or the schedule to match.
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["weather", "databricks", "ingestion"],
    doc_md=__doc__,
)
def data_load_daily():
 
    @task
    def read_api_data(ds: str | None = None) -> str:
        """Page through the API and write results to a JSON file.
 
        Returns the local file path (passed to downstream tasks via XCom).
        """
        DATA_DIR.mkdir(parents=True, exist_ok=True)
 
        session = _requests_session()
        all_records = []
        page = 1
 
        while True:
            payload = _fetch_page(session, page)
            last_page = payload["last_page"]
            print(f"Downloaded page {page}/{last_page}")
            # add page url
            page_url = f"{API_BASE_URL}?page={page}"
            new_payload = {"data": [{**record, "page_url": page_url} for record in payload["data"]]}

            all_records.extend(new_payload["data"])
 
            if page >= last_page:
                break
            if MAX_PAGES is not None and page >= MAX_PAGES:
                print(f"Hit MAX_PAGES safety cap ({MAX_PAGES}); stopping early.")
                break
 
            page += 1
 
        # Keyed off the DAG's logical date, not wall-clock time, so
        # retries/backfills overwrite the same file instead of piling up.
        local_path = DATA_DIR / f"obsoletes_{ds}.json"
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(all_records, f, indent=4, ensure_ascii=False)
 
        print(f"Saved {len(all_records)} records to {local_path}")
        return str(local_path)
 
    @task
    def upload_to_volume(local_path: str, ds: str | None = None) -> None:
        """Upload the extracted JSON file into the Databricks UC volume."""
        w = _databricks_client()
 
        filename = Path(local_path).name
        target = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{ds}/{filename}"
 
        with open(local_path, "rb") as f:
            data = f.read()
 
        w.files.upload(target, io.BytesIO(data), overwrite=True)
        print(f"Uploaded {local_path} -> {target}")
 
    @task
    def trigger_databricks_job(ds: str | None = None) -> int:
        """Kick off the downstream Databricks job for this run's date."""
        host = os.environ["DATABRICKS_HOST"]
        token = os.environ["DATABRICKS_TOKEN"]
        session = _requests_session()
 
        response = session.post(
            f"{host}/api/2.1/jobs/run-now",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "job_id": DATABRICKS_JOB_ID,
                "notebook_params": {"rundate": ds},
            },
            timeout=30,
        )
        response.raise_for_status()
        run_id = response.json()["run_id"]
        print(f"Triggered Databricks run_id: {run_id}")
        return run_id
 
    file_path = read_api_data()
    upload_to_volume(file_path) >> trigger_databricks_job()
 
 
data_load_daily()