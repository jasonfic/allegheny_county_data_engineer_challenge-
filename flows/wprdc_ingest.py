import datetime
import duckdb
from ckanapi import RemoteCKAN
from prefect import task
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    reraise=True,
)
def fetch_datastore_records(
    ckan_client: RemoteCKAN, resource_id: str, limit: int, offset: int
):
    """Paginates records from the CKAN Datastore API with exponential backoff."""
    response = ckan_client.action.datastore_search(
        id=resource_id, limit=limit, offset=offset
    )
    return response.get("records", [])


@task(name="extract-ckan-to-duckdb", retries=3, retry_delay_seconds=30)
def extract_ckan_resource(
    portal_url: str,
    resource_id: str,
    db_path: str = "/app/data/analytics.duckdb",
    page_size: int = 10000,
) -> int:
    ckan = RemoteCKAN(portal_url)
    all_records = []
    offset = 0

    while True:
        records = fetch_datastore_records(ckan, resource_id, page_size, offset)
        if not records:
            break
        all_records.extend(records)
        offset += page_size

    if not all_records:
        raise ValueError(f"No records retrieved for resource ID: {resource_id}")

    # Add ingestion audit metadata
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for row in all_records:
        row["_extracted_at"] = now
        row["_source_resource_id"] = resource_id

    # Land directly into raw DuckDB table as schema-on-read JSON/VARCHAR
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_ckan;")

    # Ingest dictionary records into a raw table
    con.register("temp_aqi", duckdb.arrow(duckdb.from_df(all_records)))
    con.execute("""
        CREATE OR REPLACE TABLE raw_ckan.daily_aqi AS 
        SELECT * FROM temp_aqi;
    """)
    con.close()

    return len(all_records)