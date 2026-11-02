# Allegheny County Data Engineer Challenge: WPRDC Pipeline with Prefect, dbt & DuckDB

This project is a multi-layered **Prefect** data pipeline built on top of a local **DuckDB** instance inside a fully containerized **Docker** environment. The initial extract/load stage is performed via a basic Python script that utilizes the ckanapi and pyarrow + duckdb libraries. Once all of the data has been loaded to a local DuckDB table via pagination, the Prefect pipeline executes a **dbt** pipeline that models the data into staging, intermediate, and mart layers. The final output is an analytic table that aggregates AQI readings to the month level, and aims to answer the question: which month had the highest number of days with unsafe air quality (defined in this case as having an air quality parameter with a maximum AQI measure above 150)?

The pipeline is designed to be triggered manually, and it can be executed multiple times in quick succession without producing duplicate data. This was accomplished using a [watermarking strategy](https://medium.com/@sujathamudadla1213/watermark-strategy-vs-change-data-capture-in-data-engineering-8f84dda21be5), which takes the maximum "date" value from the raw AQI table to determine when the most recent load occurred. The Python script then pulls all data from the API since that maximum date (inclusive), capturing all new and updated rows. It deletes any row from the raw table with an ID value that matches any incoming rows, then inserts all extracted rows. This strategy accommodates the possibilty of the AQI readings for a given day being corrected over the course of that day by deleting and replacing any rows that were loaded earlier that day.

## Architecture & Data Modeling Structure


    [Raw data from WPRDC Daily AQI Data dataset](https://data.wprdc.org/datastore/dump/4ab1e23f-3262-4bd3-adbf-f72f0119108b)
                   │
                   ▼
    Prefect-orchestrated Python script, loads data into daily_aqi table in DuckDB
                   │
                   ▼
      dbt Staging (stg_)
      (Light cleaning, typing, renaming)
                   │
                   ▼
      dbt Intermediate (int_)
      (Date parsing, day-level aggregations, and max index value calculation)
                   │
                   ▼
      dbt Fact (fct_)
      (Roll up data by month, present for analysis)
                   │
                   ▼
      dbt Test Validation
      Ensure data contains values within expected bounds via dbt's data quality check mechanism


### Design Decisions

1. Python Orchestration Tool: I chose Prefect due to its miminal infrastructural overhead compared to alternatives that required more set up or webserver configuration, like [Airflow](https://www.prefect.io/blog/airflow-local-development) and Dagster. Prefect also works very well with Python, because its control flow is handled via native Python (e.g., if/else conditionals) as opposed to the more rigid DAG format of Airflow.

2. Database Engine: I chose DuckDB over other options like PostgreSQL or SQLite because, like Prefect, it is incredibly lightweight and integrates easily with Python and dbt via well-supported libraries. DuckDB, as an OLAP database, is optimized for analytics, which fits my use case of deploying a table for the purpose of deriving insights on aggregated data. This stands in contrast to SQLite, which is also very lightweight but is not optimized for columnar, analysis-driven querying and is instead the preferred solution for embedded storage for web apps. It is also preferable to PostgreSQL, which requires client-server infrastructure overhead, which I hoped to avoid at all costs.

3. Transformation Layer: I chose dbt over a purely Python-based ETL pipeline because my source data was already stored in a tabular format and all the transforms I hoped to perform were easily achievable via SQL aggregate functions. dbt also makes data quality checks incredibly easy by providing many pre-built tests (e.g., `unique`, `not null`) and also supporting the definition of more complex tests by writing SQL queries instead of custom Python functions.
   - `assert_index_value_category_thresholds.sql`: This is a custom test I developed based on an insight I came upon from exploring the dataset. I noticed the "description" column from the source data had no explanation in the WPRDC-provided data dictionary, but after running some queries on the `index_value` ranges and doing basic research, I saw that they mapped to the [EPA's "Level of Concern" categories](https://www.airnow.gov/aqi/aqi-basics/?). I wrote this test to ensure that each `index_value` reading was mapped to the correct range as defined by the EPA, and that an error would be thrown if, e.g., an AQI reading of 105 was incorrectly labeled as "Moderate".

### Surprises in the Data

- The overall lack of days in the dataset with Unhealthy or worse AQI readings (14 days total)
- The dominance of PM2.5 as the pollutant with the highest maximum AQI values compared to CO or NO2, etc.
- The fluctuation of measurement stations over time (11 in 2016, 13 in 2019, 12 in 2026)

### What I Would Do Differently At Scale

- Daily Loads: Deploy pipeline to a running Prefect Cloud instance with a persistent backend in an analytic engine like BigQuery. Schedule jobs to run nightly at 2 AM to capture the full extent of the previous day's readings
- At 100x scale: Configure all dbt models to be materialized incrementally. Convert raw storage layer to Parquet files in cloud object store (e.g., Google Cloud Storage) partitioned on ingestion timestamp. Use dbt to upsert incoming Parquet data into heavily date-partitioned BigQuery table that is clustered on AQI parameter and measurement site.

### What I Did Not Do

- Though I initially considered it before doing research, I did not aggregate or average out AQI index values across air quality parameters (e.g., running AVG(index_value) and grouping by only date, not parameter or site). I decided not to do this because the averaged figures would be diluted and would not be an accurate reflection of reality. For example, 7/17/2026 was one of the least healthy days on record. The maximum reading for PM25_640 was over 200, but that same day also measured "Good" levels of carbon monoxide and nitrogen dioxide. Even though all 3 of those parameters have their index values measured on the same 0-300+ scale that all fall within the "Good"-"Hazardous" spectrum, it would not make sense to dilute the severity of the PM2.5 reading with the low CO reading. Think of it this way: it would not be safe to breathe in a house with a severe carbon monoxide leak just because there was low levels of dust in the air. For this reason, I chose to only look at the maximum pollutant readings when performing aggregations instead of calculating averages.
- Even though I thought it would be interesting, I did not ingest GIS data from external sources to group AQI readings into geographic zones based on the locations of measurement sites. This is because using multiple data sources was outside of scope for this project, which required I choose **one** dataset from a predetermined list. If this were expanded out into a full project, I would have compared maximum and average AQI readings based on the monitors found within Pittsburgh proper compared to the areas to the north, south, east, and west. This would be interesting based on the topographical variety of Allegheny County.
- I did not spin up any data storage or orchestration solutions that required any amount of server hosting or scalable infrastructure. This is because the data is relatively small and this project only was scoped for a singular fact table for a one-time analysis. I chose to prioritize speed and simplicity over all other considerations.

---

## Running the Pipeline

This repository is fully containerized using Docker and Docker Compose, eliminating the need to install Python, dbt, or DuckDB natively on your host machine.

### Prerequisites
* Docker Desktop (v20.10+) installed and running.

### 1. Project Setup
- Clone this repository to your local machine:
   ```
   git clone <repository-url>
   cd <repository-name>
   ```

### 2. Build and Start the Docker Environment
From the root directory of the repository, build the image and spin up the development container:

```
docker compose build
```

### 3. Run the Data Pipeline
From the same directory, trigger the pipeline (composed of the Python loader script + dbt pipeline)

```
docker compose run --rm wprdc-pipeline python -m flows.pipeline_flow
```

*Note: The pipeline uses an idempotent high-water mark that checks for the most recent load date; subsequent manual runs safely append new records without duplicating data.*

### 4. Query the Data Directly
```
# Show top 5 months in terms of number of days with Unhealthy+ AQI
docker compose run --rm wprdc-pipeline python -c "
import duckdb

# Connect in read-only mode
con = duckdb.connect('/app/data/analytics.duckdb', read_only=True)

# Preview schema and records
con.sql('SELECT * FROM main.fct_ckan__monthly_severe_aqi LIMIT 5').show(max_width=250)
"
```


## AI Usage Disclosure

This project was developed with assistance from Google's Gemini AI. Key project components such as column selection, data validation test development, debugging, data quality checks, and data exploration were done independently. Gemini was used for the following work:

- **Orchestration tool selection**: I queried Gemini to help decide between various open source Python-based orchestration solutions by listing pros and cons of each option. Using this information, I ended up choosing Prefect.

- **Prefect configuration**: While I have experience using Apache Airflow and have worked a lot with the other project components such as Python for ETL and dbt, I have never used Prefect as an orchestrator. I used Gemini to help with writing decorators, structuring my project to accommodate Prefect, and configuring my docker-compose.yml file to suppress unwanted messages from Prefect.

- **YAML schema writing**: In order to save time writing repetitive dbt schemas to go with my models, I fed my model construction queries to Gemini and asked it to develop YAML schema files to match the output columns. I refined the output to use more accurate column descriptions and added data tests beyond the generic *not_null* tests it gave me.

- **README template**: I also used Gemini to generate an outline for this very README doc and to generate markdown to separate the various sections!

Thanks for the opportunity!