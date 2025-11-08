# Spark ETL – PySpark Example (RTA ETL)

This repository contains a compact PySpark ETL application designed for learning and light production use. It demonstrates a modular layout with separate responsibilities for Spark creation, ingestion, transformation (including an observable ETL pipeline), validation, and extraction/persistence.

## Key features

- Modular components for ingestion, validation, transformation and extraction
- An "observable" ETL pipeline that captures step-level metrics and writes a `metrics_summary.json`
- Bad-record capture (written under `output/out_transportation/bad_records/` by default)
- Dynamic coalescing when writing fact tables to control output file sizes
- Optional S3 support for input/output (uses `boto3` when needed)

## Repository layout

Top-level files and purpose:

- `create_spark.py` — Creates and configures the SparkSession used by the app.
- `ingest.py` — Utilities to read CSV / Parquet input files into DataFrames.
- `transformation.py` — The ETL logic. This contains the observable RTA ETL functions and the entry `run_rta_etl_on_df(...)` that runs the pipeline on a DataFrame and writes outputs/metrics.
- `extraction.py` — Helpers to write DataFrames to disk (CSV, Parquet, etc.).
- `validate.py` — Simple validators for the Spark session and environment.
- `get_env_variables.py` — Central place for environment values (input and output paths, app name, etc.). Update this file to change runtime locations.
- `driver.py` — Main runner used in this codebase. It iterates input files under the configured source folder and invokes the ETL pipeline for each file.
- `Proporties/configeration/logging.config` — Logging configuration used by the app.

Folders:

- `source/olap/` — Put your input CSV/Parquet files here.
- `output/out_transportation/` — Default output root: dimensions, fact, bad records and `metrics_summary.json` will be written here (configurable via `get_env_variables.py`).

## Quick setup

### Prerequisites

- Python 3.8+ (depending on your Spark build)
- Java 8/11 (as required by your Spark distribution)
- PySpark installed in the Python environment used to run the scripts
- Optional: `boto3` if you plan to read/write S3 paths (install with `pip install boto3`).

### Recommended virtualenv example (Windows PowerShell):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install pyspark boto3
```

## How to run (local)

1. Put input CSV/Parquet in `source/olap/` (the repo contains an example CSV).
2. Adjust paths in `get_env_variables.py` if you want custom input/output locations.
3. Run the driver which will process every file in `source/olap/` and write outputs to the configured output root:

```powershell
python driver.py
```

## How to run with S3 / EMR

The ETL helpers detect S3-style paths (`s3://...`) and will attempt to use `boto3` for lightweight operations (uploading `metrics_summary.json` and computing temporary sizes). When running on EMR you typically run the same `driver.py` but pass S3 paths in `get_env_variables.py` or modify the code to accept CLI args.

## Configuration

`get_env_variables.py` controls:

- `src_olap` — input folder (default: `source/olap`)
- `trans_path` — output root where transformed data and metrics are written
- `envn`, `header`, `inferSchema` — small flags used by ingestion

## Important notes about input schema

The ETL pipeline expects the input data to contain certain columns (these names are used throughout the code):

- `tempRegistrationNumber` — primary key used for deduplication and joins
- `fromdate`, `todate` — used to parse event dates and produce partitioning columns
- `modelDesc`, `makerName`, `fuel`, `colour`, `vehicleClass`, `makeYear`, `seatCapacity`, `slno` — used to build dimensions/fact

If your CSV/Parquet uses different column names, either rename columns during ingestion or add a small mapping step before calling `run_rta_etl_on_df(...)`.

## Output produced

- `gold_dim_vehicle/`, `gold_dim_manufacturer/`, `gold_dim_rta/` — dimension Parquet datasets
- `gold_fact_registrations/` — coalesced fact table partitioned by registration year
- `bad_records/` — directories for `missing_key`, `duplicates_removed`, `invalid_dates`, etc.
- `metrics_summary.json` — JSON file with step-level metrics for the run

## Logging and observability

- The project uses the `logging` module configured by `Proporties/configeration/logging.config`.
- The ETL writes metrics (counts, dedupe statistics, coalesce parts and bytes) into `metrics_summary.json` for post-run inspection.

## Troubleshooting

- If you see `ImportError` for `boto3`, either install `boto3` or avoid S3 paths.
- If the driver fails with missing modules (for example `persist`), ensure those files exist in the project root or remove those imports if unused.
- For schema errors, print `df.columns` from the ingestion step to confirm column names.

## Next steps & suggestions

- Add a lightweight `requirements.txt` containing `pyspark` and optionally `boto3`.
- Add a small smoke test that runs `run_rta_etl_on_df` on a tiny sample file and asserts output exists.
- Parameterize `driver.py` to accept CLI arguments for `input_path`, `output_root`, `mode`, and `run_id` (this makes local testing and CI easier).
- If you plan to deploy on EMR or Glue, add a small wrapper script that sets appropriate Spark config and dependencies.

## Contact / notes

If you want, I can:
- run a one-off smoke run locally and collect the logs/errors,
- add a `requirements.txt` and a smoke test,
- parameterize the driver for CLI args and S3 runs.

---
_This README was expanded to document the observable ETL flow and to give actionable setup and run instructions. Update `get_env_variables.py` to customize the input/output locations for your environment._

