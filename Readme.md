# OpsPulse — Azure Batch ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4?logo=microsoftazure&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-data%20processing-150458?logo=pandas&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-containerized-2496ED?logo=docker&logoColor=white)
![Status](https://img.shields.io/badge/status-complete-brightgreen)

A Python batch ETL pipeline that ingests NYC Yellow Taxi trip data, loads it into Azure Data Lake Storage Gen2, applies data quality transformations, and produces an analysis-ready processed dataset. Built with incremental loading, structured logging, error handling, and Docker support for consistent, portable execution.

## Overview

The pipeline follows a two-zone storage pattern common in production data lakes:

- **Raw zone** — source data is landed as-is, preserving an unmodified copy for auditability and reprocessing.
- **Processed zone** — cleaned, validated data ready for downstream consumption (reporting, analytics, further transformation).

Each run is incremental: a watermark tracks the last processed timestamp, so only new records are picked up on subsequent runs. This makes the pipeline safe to re-run without producing duplicate data.

## Architecture

```
Local parquet file
        |
        v
Azure ADLS Gen2 (raw container)         -- unmodified source data
        |
        v
Transform: null handling, row validation
        |
        v
Azure ADLS Gen2 (processed container)   -- clean, analysis-ready data
```


## Tech Stack

- Python 3.13
- pandas / pyarrow for data processing
- Azure Data Lake Storage Gen2
- `azure-storage-file-datalake` SDK
- Python `logging` module for structured, persistent logs
- Docker — containerized for consistent execution across environments

## Design Decisions

**Incremental loading.** A watermark (`watermark.json`) stores the timestamp of the last successfully processed record. On each run, only rows newer than the watermark are extracted and loaded. The watermark is only updated when new data was actually processed — an earlier version of this pipeline updated it unconditionally, which produced an invalid timestamp on a run with zero new rows and silently broke incremental filtering on the following run. That's now guarded against explicitly.

**Idempotency.** Re-running the pipeline against unchanged source data results in zero new rows processed. Uploads use `overwrite=True`, so re-running against the same file does not create duplicates in storage.

**Logging over print statements.** All pipeline steps write to both stdout and a persistent `etl_log.log` file with timestamps and severity levels, so scheduled or unattended runs can be audited after the fact.

**Fail loud, not silent.** Core pipeline logic runs inside a single `try/except` block. Failures are caught, logged with context, and the pipeline exits cleanly rather than crashing with an unhandled traceback.

**Containerization.** The pipeline runs inside a Docker container built on `python:3.11-slim`, so it runs identically regardless of the host machine's Python version or installed packages. Sensitive credentials are passed at runtime via environment variables rather than being baked into the image.

## Data Quality Rules Applied

| Field | Rule |
|---|---|
| `passenger_count` | Missing values defaulted to 1 |
| `RatecodeID` | Missing values defaulted to 99 (unknown rate code) |
| `store_and_fwd_flag` | Missing values defaulted to "N" |
| `congestion_surcharge`, `Airport_fee` | Missing values defaulted to 0 |
| `fare_amount` | Rows with values <= 0 dropped |
| `trip_distance` | Rows with values <= 0 dropped |

## Assumptions

- Source data arrives as a single parquet file per run, containing a `tpep_pickup_datetime` column used as the incremental watermark field.
- The target storage account has Hierarchical Namespace enabled (ADLS Gen2). Containers are created automatically if they do not already exist.
- Rows failing basic validity checks (non-positive fare or distance) are excluded from the processed layer rather than corrected, since there is no reliable way to infer the correct value.

## Failure Scenarios

| Scenario | Observed behavior | Mitigation |
|---|---|---|
| Network interruption mid-upload | Connection or timeout error during the PATCH request | Extended `connection_timeout` (600s); failure is caught, logged, and does not corrupt partial state |
| Invalid or expired connection string | Authentication error on client initialization | Caught by exception handler, logged with a clear cause before exit |
| Missing or corrupted source file | File read fails before any network call is made | Caught by exception handler; no partial or invalid data reaches cloud storage |
| Re-run with no new data | Watermark filter returns an empty dataframe | Upload steps are skipped entirely; watermark is left untouched rather than being overwritten with an invalid value |

## Project Structure

azure_batch_etl/
opspulse_azure_upload.ipynb step-by-step pipeline, notebook form
opspulse_azure_upload.py consolidated script version
Dockerfile container build definition
requirements.txt Python dependencies
docker-compose.yaml container orchestration config
README.md
.gitignore


## Running the Pipeline

### Option 1: Run locally with Python

```bash
pip install -r requirements.txt
python opspulse_azure_upload.py
```

Before running, set the following environment variable:
- `AZURE_STORAGE_CONNECTION_STRING` — Azure Storage account connection string

### Option 2: Run with Docker

Build the image:
```bash
docker build -t opspulse-etl .
```

Run the container, passing the connection string as an environment variable and mounting a local folder containing the source data:

```bash
docker run -e AZURE_STORAGE_CONNECTION_STRING=<your-connection-string> -v <local-data-path>:/data opspulse-etl
```

This keeps credentials out of the image itself — they're supplied only at runtime.

## Possible Extensions

- Move configuration (paths, container names, watermark column) into a config file rather than hardcoding
- Add unit tests around the transformation and watermark logic
- Orchestrate with Azure Data Factory, calling this script as a Databricks or Azure Function activity
- Push the Docker image to Azure Container Registry and deploy as an Azure Container Instance for scheduled runs
