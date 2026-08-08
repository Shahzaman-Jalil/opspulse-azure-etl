# OpsPulse — Azure Batch ETL Pipeline

## Overview
This project ingests NYC Yellow Taxi trip data, uploads it to Azure Data Lake Storage Gen2 (raw layer), cleans it, and loads the cleaned version into a processed layer. The pipeline supports incremental loading, so re-runs only process new data.

## Architecture
```
Local parquet file
    -> Azure ADLS Gen2 "raw" container   (original data, untouched)
    -> Transform (clean nulls, remove invalid rows)
    -> Azure ADLS Gen2 "processed" container   (cleaned data)
```

## Tech Stack
- Python (pandas, azure-storage-file-datalake)
- Azure Data Lake Storage Gen2
- Logging via Python's `logging` module

## Key Features

### Incremental Loading
A watermark (`watermark.json`) stores the timestamp of the last successfully processed record. On each run, only records newer than the watermark are processed. This avoids reprocessing the same data and prevents duplication in downstream reports.

### Idempotency
Re-running the pipeline without new source data results in zero new rows being processed (verified: watermark correctly returned `0` new rows on a repeat run with no new data). File uploads also use `overwrite=True`, so re-uploading the same file name replaces rather than duplicates it.

### Logging
All pipeline steps are logged to both the console and a persistent `etl_log.log` file, with timestamps and severity levels (INFO/ERROR), so pipeline runs can be reviewed after the fact — including unattended/scheduled runs.

### Error Handling
The pipeline wraps all major steps in a single `try/except` block. Failures are logged with a clear message rather than crashing silently.

## Assumptions
- Source data arrives as a single parquet file per run, with a `tpep_pickup_datetime` column used for the incremental watermark.
- The Azure storage account has Hierarchical Namespace enabled (ADLS Gen2), and both `raw` and `processed` containers already exist or will be created by the pipeline on first run.
- Rows with `fare_amount <= 0` or `trip_distance <= 0` are considered invalid and are dropped during transformation.
- Missing values in `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, and `Airport_fee` are filled with sensible defaults rather than dropped, since these are non-critical fields.

## Known Failure Scenarios & Handling

| Scenario | Behavior | Handling |
|---|---|---|
| Network interruption during upload | Upload fails with a timeout/connection error | `connection_timeout=600` gives extended time; failure is caught and logged rather than crashing |
| Invalid or expired connection string | Authentication fails immediately | Caught by `try/except`, logged as an authentication error |
| Missing or corrupted source file | File read fails before any upload happens | Caught by the generic exception handler; no partial/corrupt data reaches the cloud |

## How to Run
```bash
pip install pandas pyarrow azure-storage-file-datalake
python opspulse_full_etl.py
```
Update the `connection_string` and `LOCAL_FILE_PATH` variables before running.