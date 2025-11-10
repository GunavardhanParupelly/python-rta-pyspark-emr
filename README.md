# RTA ETL Pipeline

A scalable ETL pipeline for RTA data, built with PySpark.

---

## Project Structure

```
spark1-master/
├── .vscode/               # VS Code workspace settings
├── data/                  # Input and output data files
├── output/                # Pipeline output files
├── src/                   # Source code and configuration
│   ├── config/            # Configuration files
│   │   ├── logging.config
│   │   ├── environment.py
│   │   ├── spark_config.py
│   └── core/              # Core ETL modules
│       ├── transformation.py
│       ├── extraction.py
│       ├── ingestion.py
│       ├── persist.py
│       └── validate.py
├── tests/                 # Unit and integration tests
│   └── test_transformation.py
├── .gitignore             # Git ignore rules
├── application.log        # Application log file
├── driver.py              # Main entry point for running the pipeline
├── pytest.ini             # Pytest configuration
├── requirements.txt       # Python dependencies
├── DEVELOPMENT_TRACK.md   # Development issues and solutions
├── DEVELOPMENT.md         # Additional development documentation
├── README.md              # Project overview and instructions
├── spark1locanvenv/       # Python virtual environment folder (do not commit)
```

---

## Getting Started

1. **Clone the repository**
2. **Set up your Python virtual environment**
   ```bash
   python -m venv spark1locanvenv
   .\spark1locanvenv\Scripts\activate
   pip install -r requirements.txt
   ```
3. **Configure your environment**
   - Edit files in `src/config/` as needed for your setup.

4. **Run the pipeline**
   ```bash
   python driver.py
   ```

5. **Run tests**
   ```bash
   pytest tests/
   ```

---

## Key Components

- **src/config/**: Centralized configuration for logging, environment, and Spark.
- **src/core/**: Core ETL logic and transformations (`transformation.py`, `extraction.py`, `ingestion.py`, `persist.py`, `validate.py`).
- **tests/**: Automated tests for data quality and ETL logic.
- **data/**: Input and output datasets.
- **output/**: Pipeline output files.
- **driver.py**: Main script to run the ETL pipeline.

---

## Notes

- Do **not** commit sensitive information (like AWS keys) to the repository.
- Do **not** commit your virtual environment folder (`spark1locanvenv/`) to git.
- `.pytest_cache` and other temporary files should not be tracked by git.

---

Feel free to expand this README as your project evolves!

