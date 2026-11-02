import datetime
import duckdb
import pyarrow as pa
from typing import Optional
from ckanapi import RemoteCKAN
from prefect import task
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=4, max=60),
    reraise=True,
)

def get_current_watermark(conn: duckdb.DuckDBPyConnection, table_name: str) -> Optional[str]:
    """Inspects DuckDB to check if the table exists and finds the latest date to inform subsequent loads."""
    table_check = conn.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.tables 
        WHERE table_schema = 'raw_ckan' AND table_name = '{table_name}';
    """).fetchone()[0]

    if table_check == 0:
        return None

    max_date = conn.execute(f"SELECT MAX(date) FROM raw_ckan.{table_name};").fetchone()[0]

    return max_date

def execute_ckan_sql(ckan_client: RemoteCKAN, sql: str) -> list[dict]:
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
        # Continually query API until limit is reached.
        # (Source for datastore_search_sql query: select dataset in the WPRDC site and click "Data API" > "Python" > "Querying" > "SQL query example")
        sql = f"""
            SELECT *
            FROM "{resource_id}"
            ORDER BY "_id" ASC
            LIMIT {batch_size} OFFSET {offset}
        """
        batch = execute_ckan_sql(ckan_client, sql)
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
    Ingests records from the WPRDC CKAN API and idempotently loads them to raw_ckan table in DuckDB.
    Ensures that duplicates are not loaded if pipeline is triggered multiple times
    """
    client_kwargs = {"apikey": api_key} if api_key else {}
    ckan = RemoteCKAN(portal_url, **client_kwargs)

    # Connect to DuckDB and persist
    try:
        conn = duckdb.connect(db_path)
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw_ckan;")

        max_date = get_current_watermark(conn, table_name)
        ingested_at = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # -----------------------------------------------------------------
        # Path 1: Initial Extraction (Cold Start)
        # -----------------------------------------------------------------
        if max_date is None:
            print(f"No prior run detected. Extracting all {table_name} data from CKAN...")
            records = fetch_all_records_paginated(ckan, resource_id)
            if not records:
                conn.close()
                raise ValueError(f"Resource {resource_id} returned no records.")

            for row in records:
                row["_extracted_at"] = ingested_at
                row["_source_resource_id"] = resource_id

            batch_table = pa.Table.from_pylist(records)
            conn.execute(f"""
                CREATE TABLE raw_ckan.{table_name} AS 
                SELECT * FROM batch_table;
            """)
            conn.close()
            print(f"Cold start complete. {len(records)} records loaded.")
            return len(records)

        # -----------------------------------------------------------------
        # Path 2: Incremental Extract (Inclusive Watermark)
        # -----------------------------------------------------------------
        print(f"Previous run detected. High-water mark date: {max_date}")
        
        # Query for records updated or added on or after the watermark date
        sql = f"""
            SELECT * FROM "{resource_id}"
            WHERE "date" >= '{max_date}'
            ORDER BY "_id" ASC
        """
        records = execute_ckan_sql(ckan, sql)

        if not records:
            print("No new or modified records found in CKAN.")
            conn.close()
            return 0

        for row in records:
            row["_extracted_at"] = ingested_at
            row["_source_resource_id"] = resource_id

        batch_table = pa.Table.from_pylist(records)

        # Idempotent upsert to raw table, referenced in dbt_project/models/staging/_sources.yml
        conn.begin()
        try:
            # Register incoming data
            conn.register("temp_incoming", batch_table)

            # Delete matching records using CKAN's internal primary key `_id`
            conn.execute(f"""
                DELETE FROM raw_ckan.{table_name}
                WHERE _id IN (SELECT _id FROM temp_incoming);
            """)

            # Insert new / updated versions
            conn.execute(f"""
                INSERT INTO raw_ckan.{table_name}
                SELECT * FROM temp_incoming;
            """)

            conn.commit()

        except Exception as e:
            conn.rollback()
            conn.close()
            raise RuntimeError(f"Failed to upsert records into {table_name} in DuckDB: {e}")

        conn.close()
        print(f"Idempotent update complete. {len(records)} records processed.")
        return len(records)

    finally:
        conn.close()

