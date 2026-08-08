# 🚕 OpsPulse — Azure Batch ETL Pipeline

![Python](https://img.shields.io/badge/Python-3.13-blue?logo=python&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-ADLS%20Gen2-0078D4?logo=microsoftazure&logoColor=white)
![Pandas](https://img.shields.io/badge/pandas-data%20processing-150458?logo=pandas&logoColor=white)
![Status](https://img.shields.io/badge/status-complete-brightgreen)

A Python-based batch ETL pipeline that ingests NYC Yellow Taxi trip data, lands it in Azure Data Lake Storage Gen2, cleans it, and produces an analysis-ready processed layer — with incremental loading, logging, and error handling built in.

---

## 📌 Overview

This project simulates a real-world cloud data pipeline: raw data lands untouched in a **raw** zone for auditability, gets cleaned and validated, then lands in a **processed** zone ready for downstream analytics. Re-runs are safe — the pipeline only processes new data since the last run.

## 🏗️ Architecture

```
┌─────────────────────┐
│  Local Parquet File  │   (yellow_tripdata_2024-01.parquet)
└──────────┬───────────┘
           │
           ▼
┌─────────────────────────────┐
│  Azure ADLS Gen2 — "raw"     │   original data, untouched
└──────────┬───────────────────┘
           │
           ▼
   ┌───────────────────┐
   │   Transform Layer   │   clean nulls · drop invalid rows
   └──────────┬─────────┘
              │
              ▼
┌───────────────────────────────────┐
│  Azure ADLS Gen2 — "processed"     │   analysis-ready data
└─────────────────────────────────────┘
```

## ⚙️ Tech Stack

| Category | Tools |
|---|---|
| Language | Python 3.13 |
| Data processing | pandas, pyarrow |
| Cloud | Azure Data Lake Storage Gen2 |
| SDK | `azure-storage-file-datalake` |
| Observability | Python `logging` module |

## ✨ Key Features

### 🔄 Incremental Loading
A watermark file (`watermark.json`) stores the timestamp of the last successfully processed record. Each run only picks up rows newer than that watermark — no reprocessing, no duplicate downstream data.

### ♻️ Idempotency
Re-running the pipeline with no new source data results in **zero** new rows being processed — verified end-to-end. Uploads also use `overwrite=True`, so re-uploading a file replaces it rather than duplicating it.

### 📝 Logging
Every step is logged to both the console and a persistent `etl_log.log` file, with timestamps and severity levels — so runs (including unattended/scheduled ones) can be audited after the fact.

### 🛡️ Error Handling
Core pipeline steps run inside a `try/except` block. Failures are caught and logged with a clear message instead of crashing silently.

## 📂 Project Structure

```
azure_batch_etl/
├── opspulse_azure_upload.ipynb   # step-by-step pipeline (notebook)
├── opspulse_full_etl.py          # consolidated pipeline script
├── README.md
├── .gitignore
```

## 🧠 Assumptions

- Source data arrives as a single parquet file per run, with `tpep_pickup_datetime` used as the incremental watermark column.
- The Azure storage account has Hierarchical Namespace enabled (ADLS Gen2); `raw` and `processed` containers are created automatically if they don't exist.
- Rows with `fare_amount <= 0` or `trip_distance <= 0` are treated as invalid and dropped during transformation.
- Missing values in non-critical fields (`passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge`, `Airport_fee`) are filled with sensible defaults rather than dropped.

## ⚠️ Known Failure Scenarios & Handling

| Scenario | Behavior | Handling |
|---|---|---|
| Network interruption during upload | Timeout/connection error | `connection_timeout=600` gives extended time; failure is caught and logged, not crashed |
| Invalid or expired connection string | Authentication fails immediately | Caught by `try/except`, logged as an authentication error |
| Missing or corrupted source file | File read fails before upload | Caught by the exception handler; no partial data reaches the cloud |

## 🚀 How to Run

```bash
pip install pandas pyarrow azure-storage-file-datalake
python opspulse_full_etl.py
```

Before running, update:
- `connection_string` — your Azure Storage connection string
- `local_file_path` — path to your source parquet file

## 📈 Future Improvements

- Parameterize source file paths and container names via a config file
- Add unit tests for the transform logic
- Migrate orchestration to Azure Data Factory with this script as a Databricks/Function activity

---

**Author:** Shah Zaman Jalil · [GitHub](https://github.com/Shahzaman-Jalil)
