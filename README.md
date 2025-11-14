# ⭐ RTA ETL Pipeline

A scalable and modular ETL pipeline for RTA data, built with **Python** and **PySpark**. This pipeline is designed to handle large-scale data processing, ensuring **data quality**, **fault tolerance**, and seamless integration with distributed systems like **Apache Spark**.

---

## 🌟 Features

- **Modular Design:** Clean separation of extraction, transformation, and loading (ETL) logic.
- **Scalable:** Built with PySpark to handle large datasets efficiently.
- **Data Validation:** Pre- and post-metrics tracking for data quality assurance.
- **Fault Tolerant:** Logs errors and handles invalid records gracefully.
- **Customizable:** Centralized configuration for Spark, logging, and environment settings.

---

## 📁 Project Structure

```
spark1-master/
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
├── README.md              # Project overview and instructions
```

---

## 🛠️ Key Components

1. **Configuration (config)**  
   Centralized configuration for logging, environment variables, and Spark settings.

2. **Core ETL Logic (core)**  
   - `transformation.py`: Data transformation logic.
   - `extraction.py`: Data extraction from source files.
   - `ingestion.py`: Data ingestion into Spark.
   - `persist.py`: Data persistence to output files.
   - `validate.py`: Data validation and metrics tracking.

3. **Tests (tests)**  
   Automated tests for data quality and ETL logic.

4. **Input and Output**  
   - `data`: Input and output datasets.
   - `output`: Processed data files.

---

## 📊 Metrics Tracking

The pipeline tracks pre-metrics and post-metrics to ensure data quality:

- **Pre-Metrics:** Calculated before data transformation (e.g., row counts, null counts).
- **Post-Metrics:** Calculated after data transformation to validate results.

---

## 📝 Example Workflow

1. **Extract:** Read raw data from data directory.
2. **Transform:** Clean, deduplicate, and validate the data.
3. **Load:** Write the processed data to the output directory.
4. **Validate:** Compare pre- and post-metrics to ensure data quality.

---

## 📂 Output Structure

```
output/
├── stage_clean_source/       # Staged clean data
├── gold_fact_registrations/  # Final fact table
├── gold_dim_vehicle/         # Vehicle dimension table
├── gold_dim_manufacturer/    # Manufacturer dimension table
├── gold_dim_rta/             # RTA dimension table
├── error_table/              # Invalid records
```

---

