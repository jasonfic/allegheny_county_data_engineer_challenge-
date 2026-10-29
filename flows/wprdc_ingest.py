import duckdb
from typing import Optional
from ckanapi import RemoteCKAN
from prefect import task
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    reraise=True,
)
def execute_sql(ckan_client: RemoteCKAN, sql: str) -> list[dict]:
    """
    Executes a datastore_search_sql query with exponential backoff.
    (Source: select dataset in the WPRDC site and click "Data API" > "Python" > "Querying" > "SQL query example")
    """
    response = ckan_client.action.datastore_search_sql(sql=sql)
    return response.get("records", [])


def fetch_all_records_paginated(
    ckan_client: RemoteCKAN, resource_id: str, batch_size: int = 10000
) -> list[dict]:
    """
    Safely paginates through the entire CKAN table using SQL LIMIT and OFFSET
    to bypass server-side query size caps during the initial load.
    Testing phase revealed that a SELECT * query only returns 32,000 records
    """
    all_records = []
    offset = 0

    while True:
        sql = f"""
            SELECT *
            FROM "{resource_id}"
            ORDER BY "_id" ASC
            LIMIT {batch_size} OFFSET {offset}
        """
        batch = execute_sql(ckan_client, sql)
        if not batch:
            break

        all_records.extend(batch)
        print(f"Fetched {len(all_records)} historical records so far...")
        offset += batch_size

        # If a batch returned fewer records than the limit, we reached the end
        if len(batch) < batch_size:
            break

    return all_records


@task(name="extract-ckan", retries=3, retry_delay_seconds=30)
def extract_ckan_resource(
    portal_url: str,
    resource_id: str,
    table_name: str,
    api_key: Optional[str] = None,
    db_path: str = "/app/data/analytics.duckdb",
) -> int:
    """
    Ingests records from the WPRDC CKAN API and loads them to raw_ckan table in DuckDB.
    """
    client_kwargs = {}
    if api_key:
        client_kwargs["apikey"] = api_key

    ckan = RemoteCKAN(portal_url, **client_kwargs)

    # Connect to DuckDB and persist
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw_ckan;")

    print(f"Starting full load for resource {resource_id}")
    records = fetch_all_records_paginated(ckan, resource_id)

    if not records:
        conn.close()
        return 0

    # Register in-memory data as a temporary DuckDB table
    conn.register("temp_batch", duckdb.arrow(duckdb.from_df(records)))

    # Idempotent write: ensure prior runs for the same date partition are replaced
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_ckan.{table_name} AS 
        SELECT * FROM temp_batch;
    """)

    total_count = len(records)
    conn.close()

    print(
        f"Successfully loaded {total_count} records for {table_name} into DuckDB."
    )
    return total_count