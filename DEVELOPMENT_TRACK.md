# Development Track - Issues and Solutions

This document tracks all issues encountered during the development and migration of the RTA ETL Pipeline project.

## Table of Contents
1. [Environment Setup Issues](#environment-setup-issues)
2. [PySpark Import Issues](#pyspark-import-issues)
3. [Windows-Specific Issues](#windows-specific-issues)
4. [Project Structure Migration](#project-structure-migration)
5. [Configuration Issues](#configuration-issues)
6. [Final Working Configuration](#final-working-configuration)

---

## Environment Setup Issues

### Issue 1: Multiple Python Environments
**Problem:** Confusion between system Python, virtualenv, and manual Spark installation.

**Symptoms:**
- Packages installed but not found
- Wrong Python interpreter being used

**Solution:**
```powershell
# Always use virtualenv
cd C:\Users\Gunav\Desktop\spark1-master
.\sparl1locanvenv\Scripts\Activate.ps1

# Verify correct Python
python -c "import sys; print(sys.executable)"
# Should show: C:\Users\Gunav\Desktop\spark1-master\sparl1locanvenv\Scripts\python.exe
```

**Status:** ✅ Resolved

---

## PySpark Import Issues

### Issue 2: Circular Import Error (PRIMARY ISSUE)
**Problem:** 
```
ImportError: cannot import name 'Column' from partially initialized module 'pyspark.sql'
(most likely due to a circular import)
```

**Root Cause:** 
Python was loading PySpark from manual installation at `C:\spark\spark-3.4.2-bin-hadoop3` instead of the virtualenv pip-installed version.

**Why it happened:**
1. Manual Spark installation had `SPARK_HOME` environment variable set
2. Manual Spark paths were in system `PATH`
3. `PYTHONPATH` was pointing to manual Spark installation
4. Python's module resolution prioritized system paths over virtualenv

**Failed Attempts:**
- ❌ Removing `SPARK_HOME` (kept resetting)
- ❌ Cleaning `PYTHONPATH` (not persistent across sessions)
- ❌ Reinstalling PySpark (didn't change import order)
- ❌ Editing Windows System Environment Variables (required admin/restart)

**Final Solution:**
Force correct path order at the start of `driver.py`:

```python
# filepath: c:\Users\Gunav\Desktop\spark1-master\driver.py
import os
import sys

# FORCE Python to use virtualenv packages FIRST
VENV_PATH = r'C:\Users\Gunav\Desktop\spark1-master\sparl1locanvenv\Lib\site-packages'
if VENV_PATH not in sys.path:
    sys.path.insert(0, VENV_PATH)

# Remove any manual Spark paths
sys.path = [p for p in sys.path if 'spark-3.4.2-bin-hadoop3' not in p]
```

**Status:** ✅ Resolved

**Key Lesson:** When you have conflicting installations, explicitly control `sys.path` order rather than fighting with environment variables.

---

## Windows-Specific Issues

### Issue 3: Hadoop Native Library Error
**Problem:**
```
java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$Windows.access0
```

**Root Cause:** 
Spark on Windows needs `winutils.exe` and `hadoop.dll` to perform file system operations. These native binaries bridge Java/Hadoop calls to Windows APIs.

**Solution:**
1. Download `winutils.exe` and `hadoop.dll` for Hadoop 3.3.5 from [cdarlint/winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin)

2. Place in `C:\spark\hadoop-3.3.5\bin\`

3. Set environment variables in `driver.py`:
```python
if sys.platform.startswith('win'):
    os.environ.setdefault('HADOOP_HOME', r'C:\spark\hadoop-3.3.5')
    os.environ.setdefault('JAVA_HOME', r'C:\Program Files\Java\jdk-11')
    hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    if hadoop_bin not in os.environ.get('PATH', ''):
        os.environ['PATH'] += ';' + hadoop_bin
```

**Status:** ✅ Resolved

**Note:** The `ShutdownHookManager` temp directory cleanup errors are harmless and can be ignored.

---

## Project Structure Migration

### Issue 4: Flat Project Structure
**Problem:** 
All Python files in root directory with no clear organization:
```
spark1-master/
├── driver.py
├── create_spark.py
├── get_env_variables.py
├── ingest.py
├── transformation.py
├── validate.py
├── persist.py
└── extraction.py
```

**Solution:** 
Migrated to modular `src/` structure:
```
spark1-master/
├── driver.py
├── src/
│   ├── config/
│   │   ├── spark_config.py      # (was create_spark.py)
│   │   └── environment.py        # (was get_env_variables.py)
│   ├── core/
│   │   ├── ingest.py
│   │   ├── transformation.py
│   │   ├── validate.py
│   │   └── persist.py
│   └── utils/
│       └── logger.py
└── data/
    ├── input/                    # (was source/olap/)
    └── output/
```

**Migration Steps:**
```powershell
# Create structure
New-Item -ItemType Directory -Path "src\config", "src\core", "src\utils" -Force
New-Item -ItemType File -Path "src\__init__.py", "src\config\__init__.py", "src\core\__init__.py", "src\utils\__init__.py" -Force

# Copy files
Copy-Item "create_spark.py" "src\config\spark_config.py"
Copy-Item "get_env_variables.py" "src\config\environment.py"
# ... etc
```

**Status:** ✅ Completed

---

## Configuration Issues

### Issue 5: Missing PySpark Functions Import
**Problem:**
```python
NameError: name 'F' is not defined
```
At line 115 in `transformation.py`:
```python
df.withColumn("row_num", F.row_number().over(window_spec))
```

**Root Cause:** 
Missing import statement for PySpark SQL functions.

**Solution:**
Added to `src/core/transformation.py`:
```python
from pyspark.sql import functions as F
from pyspark.sql.functions import col
```

**Status:** ✅ Resolved

---

### Issue 6: Logging Configuration Path
**Problem:**
```
FileNotFoundError: Proporties/configeration/logging.config doesn't exist
```

**Root Causes:**
1. Folder name typo: `Proporties` instead of `Properties`
2. Folder name typo: `configeration` instead of `configuration`
3. Hardcoded path in multiple files
4. Path not adjusted after project restructuring

**Files affected:**
- `src/config/spark_config.py`
- `src/core/ingest.py`
- Other core files

**Solution:**
Removed hardcoded logging config from individual modules:
```powershell
# Automated fix for all files
Get-ChildItem -Path "src" -Recurse -Include *.py | ForEach-Object {
    $content = Get-Content $_.FullName -Raw
    $content = $content -replace "logging\.config\.fileConfig\('Proporties/configeration/logging\.config'\)", "# logging.config.fileConfig removed"
    $content | Set-Content $_.FullName -Encoding UTF8
}
```

Centralized logging setup in `src/utils/logger.py`:
```python
def setup_logging(config_path=None):
    if config_path is None:
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(project_root, 'config', 'logging.config')
    
    if os.path.isfile(config_path):
        logging.config.fileConfig(config_path)
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    return logging.getLogger(__name__)
```

**Status:** ✅ Resolved

---

### Issue 7: Parameter Naming Inconsistency
**Problem:** 
Function parameter named `file_formate` (typo) instead of `file_format`.

**Location:** 
`src/core/ingest.py` - `load_files()` function

**Decision:** 
Kept the typo for backward compatibility since it was used consistently throughout the codebase.

**Status:** ⚠️ Known issue (not breaking, can be refactored later)

**Future refactoring:**
```python
# Change signature
def load_files(spark, file_format, inferSchema, file_dir, header):  # fixed typo
    # ...

# Update all call sites in driver.py
```

---

### Issue 8: Multiple requirements.txt Files
**Problem:** 
Found `requirements.txt` in both project root and `output/out_transportation/` directory.

**Root Cause:** 
Accidentally generated during ETL pipeline run.

**Solution:**
```powershell
# Remove incorrect file
Remove-Item "output\out_transportation\requirements.txt" -Force

# Keep root requirements.txt with:
# pyspark==3.4.2
# py4j==0.10.9.7
# boto3>=1.26.0
```

**Status:** ✅ Resolved

---

## Final Working Configuration

### Environment Setup
```powershell
# 1. Navigate to project
cd C:\Users\Gunav\Desktop\spark1-master

# 2. Activate virtualenv
.\sparl1locanvenv\Scripts\Activate.ps1

# 3. Verify PySpark location (should be in virtualenv)
python -c "import pyspark; print(pyspark.__file__)"
# Expected: C:\Users\Gunav\Desktop\spark1-master\sparl1locanvenv\Lib\site-packages\pyspark\__init__.py

# 4. Run pipeline
python driver.py
```

### Key Files Configuration

**driver.py** (top of file):
```python
import os
import sys

# FORCE virtualenv PySpark first
VENV_PATH = r'C:\Users\Gunav\Desktop\spark1-master\sparl1locanvenv\Lib\site-packages'
if VENV_PATH not in sys.path:
    sys.path.insert(0, VENV_PATH)

# Remove manual Spark paths
sys.path = [p for p in sys.path if 'spark-3.4.2-bin-hadoop3' not in p]

# Windows setup (Hadoop native libraries)
if sys.platform.startswith('win'):
    os.environ.setdefault('HADOOP_HOME', r'C:\spark\hadoop-3.3.5')
    os.environ.setdefault('JAVA_HOME', r'C:\Program Files\Java\jdk-11')
    hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    if hadoop_bin not in os.environ.get('PATH', ''):
        os.environ['PATH'] += ';' + hadoop_bin

# Add src to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(PROJECT_ROOT, 'src')
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)
```

**src/core/transformation.py** (imports):
```python
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp, year, to_date
```

**src/config/spark_config.py** (cleaned):
```python
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def get_spark_object(env, app_name):
    # ... (no logging.config.fileConfig call)
```

---

## Lessons Learned

### 1. Virtual Environment Isolation
**Problem:** Multiple Python/PySpark installations causing conflicts.

**Lesson:** Always explicitly control `sys.path` when dealing with conflicting installations. Don't rely solely on environment variables.

### 2. Windows Hadoop Support
**Problem:** Native library errors on Windows.

**Lesson:** Windows Spark development requires `winutils.exe`. Always set `HADOOP_HOME` for Windows environments.

### 3. Project Structure
**Problem:** Flat structure made code organization difficult.

**Lesson:** Use `src/` structure from the start. Organize by responsibility (config, core, utils).

### 4. Configuration Management
**Problem:** Hardcoded paths in multiple files.

**Lesson:** Centralize configuration and use relative paths from project root.

### 5. Import Consistency
**Problem:** Missing imports caused runtime failures.

**Lesson:** Use explicit imports. Add type hints and use linters (pylint, mypy) to catch these early.

---

## Development Timeline

| Date | Issue | Status |
|------|-------|--------|
| 2025-11-08 | Circular import error discovered | ❌ |
| 2025-11-08 | Attempted environment variable fixes | ❌ |
| 2025-11-08 | Implemented sys.path forcing | ✅ |
| 2025-11-08 | Windows Hadoop native library error | ✅ |
| 2025-11-08 | Missing F import in transformation.py | ✅ |
| 2025-11-08 | Project structure migration started | ✅ |
| 2025-11-08 | Logging configuration issues | ✅ |
| 2025-11-08 | All issues resolved | ✅ |

---

## Current Status: ✅ WORKING

**Working configuration verified:**
- ✅ PySpark loads from virtualenv
- ✅ No circular import errors
- ✅ Windows Hadoop native libraries functional
- ✅ Project structure organized in `src/`
- ✅ All imports resolved
- ✅ ETL pipeline runs successfully

**Next Steps:**
1. Add unit tests
2. Add type hints
3. Set up CI/CD
4. Refactor `file_formate` typo
5. Add comprehensive docstrings
6. Create deployment guide for production

---

## Quick Reference: Common Commands

```powershell
# Activate environment
.\sparl1locanvenv\Scripts\Activate.ps1

# Verify PySpark location
python -c "import pyspark; print(pyspark.__file__)"

# Run pipeline
python driver.py

# Check Python path
python -c "import sys; [print(p) for p in sys.path]"

# Reinstall PySpark (if needed)
pip uninstall pyspark py4j -y
pip install pyspark==3.4.2

# Check environment variables
echo $env:SPARK_HOME
echo $env:HADOOP_HOME
echo $env:PYTHONPATH
```

---

**Document maintained by:** Development Team  
**Last updated:** 2025-11-08  
**Project:** RTA ETL Pipeline (spark1-master)
