import subprocess
from prefect import flow, task
from flows.wprdc_ingest import extract_ckan_resource


@task(name="run-dbt-models")
def run_dbt():
    cmd = [
        "dbt",
        "run",
        "--project-dir",
        "/app/dbt_project",
        "--profiles-dir",
        "/app/config",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    print(result.stdout)


@task(name="test-dbt-models")
def test_dbt():
    cmd = [
        "dbt",
        "test",
        "--project-dir",
        "/app/dbt_project",
        "--profiles-dir",
        "/app/config",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    print(result.stdout)


@flow(name="wprdc_to_duckdb_dbt_pipeline")
def pipeline(portal_url: str, resource_id: str, table_name: str):
    # 1. Ingestion
    record_count = extract_ckan_resource(
        portal_url=portal_url, resource_id=resource_id
    )
    print(f"Ingested {record_count} records into raw_ckan.{table_name}")

    # 2. Transformations
    run_dbt()

    # 3. Validation
    test_dbt()


if __name__ == "__main__":
    # Example invocation using a CKAN endpoint and target resource
    pipeline(
        portal_url="https://data.wprdc.org",
        resource_id="4ab1e23f-3262-4bd3-adbf-f72f0119108b",
        table_name="daily_aqi"
    )