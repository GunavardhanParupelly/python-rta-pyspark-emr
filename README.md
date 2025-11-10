# Spark ETL – PySpark Example (RTA ETL)

Production-ready PySpark ETL pipeline for vehicle registration data processing with modular architecture.

## Project Structure

```
spark1-master/
├── src/
│   ├── config/              # Configuration modules
│   │   ├── spark_config.py  # Spark session setup
│   │   └── environment.py   # Environment variables
│   ├── core/                # Core ETL logic
│   │   ├── ingest.py        # Data loading
│   │   ├── transformation.py# ETL transformations
│   │   ├── validate.py      # Data validation
│   │   └── persist.py       # Output writing
│   └── utils/               # Utility functions
│       └── logger.py        # Logging setup
├── data/
│   ├── input/               # Input CSV/Parquet files
│   └── output/              # Processed output
├── config/
│   └── logging.config       # Logging configuration
├── tests/                   # Unit tests
├── driver.py                # Main entry point
├── requirements.txt
└── README.md
```

## Quick Start

### 1. Setup Environment

```powershell
# Clone/navigate to project
cd C:\Users\Gunav\Desktop\spark1-master

# Create virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Paths

Edit `src/config/environment.py` to customize:
- Input/output directories
- Spark configurations
- Application settings

### 3. Add Input Data

Place CSV/Parquet files in `data/input/`

### 4. Run Pipeline

```powershell
python driver.py
```

## Configuration

### Environment Variables (`src/config/environment.py`)

```python
envn = 'LOCAL'                    # Environment: LOCAL, DEV, PROD
appName = 'RTA_ETL_Pipeline'      # Application name
src_olap = 'data/input'           # Input directory
trans_path = 'data/output'        # Output directory
header = 'True'                   # CSV has header
inferSchema = 'True'              # Auto-detect schema
```

### Windows-Specific Setup

Ensure these are configured (already handled in `driver.py`):
- `HADOOP_HOME`: Path to Hadoop binaries with winutils.exe
- `JAVA_HOME`: Path to JDK 8/11/17

## Output

The pipeline generates:
- **Dimension tables**: `gold_dim_vehicle/`, `gold_dim_manufacturer/`, `gold_dim_rta/`
- **Fact table**: `gold_fact_registrations/` (partitioned by year)
- **Bad records**: Separate folders for duplicates, missing keys, invalid dates
- **Metrics**: `metrics_summary.json` with pipeline statistics

## Development

### Running Tests

```powershell
pytest tests/
```

### Adding New Transformations

1. Add transformation function to `src/core/transformation.py`
2. Import and call in `driver.py` or create new pipeline script
3. Add tests in `tests/`

### Code Style

Follow PEP 8. Use docstrings for all functions:

```python
def my_function(param1, param2):
    """
    Brief description
    
    Args:
        param1: Description
        param2: Description
        
    Returns:
        Description of return value
    """
    pass
```

## Troubleshooting

See detailed troubleshooting guide in [TROUBLESHOOTING.md](TROUBLESHOOTING.md) or README Troubleshooting section.

## License

MIT License (or your preferred license)

