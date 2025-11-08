# Development Tracker & Change Summary

This file captures the sequence of work performed while integrating the supplied ETL script into this repository, issues discovered, and how they were resolved. Keep this file as a single source-of-truth for what was changed, why, and how to reproduce runs/tests locally.

## High-level summary
- Integrated a standalone PySpark ETL script into the repo by moving transformation logic into `transformation.py` and wiring the pipeline from `driver.py`.
- Added test coverage (pytest) and a Spark fixture for local unit testing.
- Addressed Windows-native Hadoop / Parquet commit failures by supporting CSV outputs and configuring `winutils.exe` (HADOOP_HOME).
- Added smaller helpers and scaffolding (persist stub, requirements, vscode settings) and improved logging and error handling.

## Chronological actions (what was done)
1. Added ETL primitives into `transformation.py` (parse_dates, clean_and_stage, derive_emission_standard, generate_surrogate_keys, build_dimensions, fuzzy_vehicle_resolution, assemble_fact_table, dynamic_coalesce_write, run_rta_etl_on_df).
2. Updated `driver.py` to call the new runner `run_rta_etl_on_df` and removed old preview code.
3. Created a minimal `persist.py` with a `save_parquet` stub to avoid import errors while iterating.
4. Added `requirements.txt` (pyspark, boto3, pytest) and created the venv `sparl1locanvenv` for local testing.
5. Diagnosed a Windows-specific failure: `java.lang.UnsatisfiedLinkError` thrown from `org.apache.hadoop.io.nativeio.NativeIO$Windows.access0` during DataFrame write commit.
6. Implemented output-format support (CSV vs Parquet) and updated bad-record writes to respect `output_format='csv'` to avoid Parquet temp-stage issues on Windows.
7. Installed and pointed Spark to `winutils.exe` (HADOOP_HOME) to allow Hadoop native calls on Windows; added optional programmatic setup to `driver.py` to set `HADOOP_HOME` when running on Windows.
8. Fixed logic bug in fuzzy matching where `VEHICLE_ID` caused ambiguous reference; renamed the lookup vehicle id to `DV_VEHICLE_ID` and updated ordering.
9. Repaired and extended unit tests, added `tests/` with a `conftest.py` Spark fixture and multiple unit tests covering parsing, surrogate key generation, assembly, clean_and_stage, fuzzy matching, and dynamic coalesce write.
10. Ran tests and iterated until all tests passed locally.

## Files added or significantly changed
- `transformation.py` — main ETL logic (many functions added/updated). Key changes: output_format propagation, safe writes, fuzzy matching fix, dynamic coalesce.
- `driver.py` — wired to `run_rta_etl_on_df`; added programmatic HADOOP_HOME setup for Windows.
- `persist.py` — small helper stub to satisfy imports.
- `requirements.txt` — project dependencies (pyspark, boto3, pytest).
- `DEVELOPMENT_TRACKER.md` — this file.
- `tests/conftest.py` — pytest SparkSession fixture (sets HADOOP_HOME on Windows for tests).
- `tests/test_transformation.py`, `tests/test_clean_and_stage.py`, `tests/test_fuzzy_and_coalesce.py` — unit tests added.
- `Proporties/configeration/logging.config` — logger levels adjusted to capture INFO logs in `application.log`.

## Problems encountered & how we fixed them

- Problem: Parquet/CSV writes failed on Windows with UnsatisfiedLinkError (NativeIO$Windows.access0).
  - Cause: Hadoop native library calls (winutils/nativeio) were missing or not visible to the JVM. FileOutputCommitter calls these on commit.
  - Fixes applied:
    1. Short-term: added `output_format='csv'` support and made bad-record writes write CSV when requested so some code paths avoid Parquet temp-stage.
    2. Durable/final: placed a `winutils.exe` (from Hadoop distribution) on the machine and set `HADOOP_HOME` to the folder containing `bin\winutils.exe` (example in this repo uses `C:\spark\hadoop-3.3.5`). This resolves native call failures.
    3. Programmatic convenience: added a small snippet in `driver.py` and `tests/conftest.py` to set `HADOOP_HOME` on Windows so local dev doesn't need manual steps every time.

- Problem: Ambiguous column error during fuzzy matching / assembly ([AMBIGUOUS_REFERENCE] Reference `VEHICLE_ID`).
  - Fix: In the fuzzy-lookup step renamed the veh lookup id to `DV_VEHICLE_ID` and used it for tie-breaking and fuzzy selection; updated assembly logic to reference `r.VEHICLE_ID_resolved` and `r.IS_FUZZY_MATCH` consistently.

- Problem: Tests failing due to missing columns or schema mismatch.
  - Fix: Extended test fixtures to create DataFrames with the full schema used by the production code (e.g., `fuel`, `colour`, `vehicleClass`, `makeYear`, `seatCapacity`, `secondVehicle`), and included `IS_FUZZY_MATCH` in resolved maps used by `assemble_fact_table`.

## How to run locally (dev quick-start)
1. Create and activate the venv (already created in this workspace as `sparl1locanvenv`):
   - PowerShell:
     ```powershell
     .\sparl1locanvenv\Scripts\Activate.ps1
     python -m pip install --upgrade pip
     python -m pip install -r requirements.txt
     ```

2. Ensure `winutils.exe` is available on Windows and set `HADOOP_HOME` (one-time):
   - Copy `winutils.exe` to `C:\spark\hadoop-3.3.5\bin\winutils.exe` (or any folder you prefer).
   - Set env for current session:
     ```powershell
     $env:HADOOP_HOME = 'C:\spark\hadoop-3.3.5'
     $env:PATH = $env:PATH + ';C:\spark\hadoop-3.3.5\bin'
     ```
   - Or accept the programmatic setup in `driver.py` (the script sets `HADOOP_HOME` automatically when run on Windows).

3. Run the ETL (from repository root):
   ```powershell
   .\sparl1locanvenv\Scripts\python.exe driver.py
   ```

4. Run tests:
   ```powershell
   .\sparl1locanvenv\Scripts\python.exe -m pytest tests -q
   ```

## Tests added & coverage notes
- Unit tests added cover:
  - `parse_dates` parsing logic
  - `generate_surrogate_keys` creates VEHICLE_ID/MANUFACTURER_ID/RTA_ID
  - `assemble_fact_table` happy path
  - `clean_and_stage` handling of missing keys, duplicates, and invalid dates (basic)
  - `fuzzy_vehicle_resolution` basic path and `dynamic_coalesce_write` for CSV output

- These tests exercise core primitives but do not yet cover:
  - full dynamic coalesce Parquet path (environment-sensitive on Windows)
  - S3 interactions (mock or integration tests needed)
  - large-scale partitioning/coalesce heuristics beyond small-sample behavior

## Logs & artifacts
- `application.log` captures run logs, metrics, and stack traces. It was truncated and backed up during the work; an archived copy exists at `application.log.bak_*`.

## Recommended next steps (low-risk improvements)
1. Add a CI workflow (GitHub Actions) that installs the venv, sets HADOOP_HOME (or runs in Linux), and runs `pytest`.
2. Centralize write logic behind a helper (e.g., `io.write_df(df, path, format, mode)`) and make tests mock or assert that helper calls were invoked instead of performing FS commits.
3. Add integration test that runs the full pipeline on a tiny CSV and verifies output files and metrics.
4. Add data-quality checks and unit tests for edge cases in `derive_emission_standard` and `build_dimensions`.

## Notes / Assumptions
- This tracker assumes a local Spark 3.x distribution compatible with the code in this repository (the workspace used Spark 3.4.2 with Hadoop3).
- On Windows, `winutils.exe` matching the Hadoop version is required to allow DataFrame write commits.

If you'd like, I can convert this tracker into a chronological changelog file (git-friendly), add a GitHub Actions CI config, or centralize the writer helper and update tests accordingly. Tell me which you'd prefer next.
