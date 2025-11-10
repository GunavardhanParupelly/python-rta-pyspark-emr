"""
Environment configuration and variables
"""
import os

# Application settings
envn = 'LOCAL'
appName = 'RTA_ETL_Pipeline'

# Data paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
src_olap = os.path.join(PROJECT_ROOT, 'data', 'input')
trans_path = os.path.join(PROJECT_ROOT, 'data', 'output', 'out_transportation')

# File reading options
header = 'True'
inferSchema = 'True'

# Spark configurations
spark_configs = {
    'spark.sql.shuffle.partitions': '10',
    'spark.sql.adaptive.enabled': 'true'
}

