"""
Main driver script for RTA ETL Pipeline
"""
import os
import sys
import time
from time import perf_counter
import logging.config

# FORCE Python to use virtualenv packages FIRST
VENV_PATH = r'C:\Users\Gunav\Desktop\spark1-master\sparl1locanvenv\Lib\site-packages'
if VENV_PATH not in sys.path:
    sys.path.insert(0, VENV_PATH)

# Remove any manual Spark paths
sys.path = [p for p in sys.path if 'spark-3.4.2-bin-hadoop3' not in p]

# Windows setup
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

# Now import everything
from config.environment import envn, appName, src_olap, trans_path, header, inferSchema
from config.spark_config import get_spark_object
from core.ingest import load_files, display_df, df_count
from core.transformation import run_rta_etl_on_df
from core.validate import get_current_date
from utils.logger import setup_logging

# Setup logging
logging.config.fileConfig('src/config/logging.config')
logger = setup_logging()


def main():
    """
    Main ETL pipeline execution
    """
    start = perf_counter()
    
    try:
        logger.info('=' * 80)
        logger.info('Starting RTA ETL Pipeline')
        logger.info('=' * 80)
        
        # Create Spark session
        logger.info('Creating Spark session...')
        spark = get_spark_object(envn, appName)
        get_current_date(spark)
        
        # Verify input directory
        if not os.path.isdir(src_olap):
            logger.error(f'Input directory not found: {src_olap}')
            return 1
        
        # Process each file
        files_processed = 0
        for file in os.listdir(src_olap):
            file_path = os.path.join(src_olap, file)
            
            if not os.path.isfile(file_path):
                continue
            
            # Determine file format
            if file.lower().endswith('.parquet'):
                file_format = 'parquet'
                file_header = 'NA'
                file_infer_schema = 'NA'
            elif file.lower().endswith('.csv'):
                file_format = 'csv'
                file_header = header
                file_infer_schema = inferSchema
            else:
                logger.info(f'Skipping unsupported file: {file}')
                continue
            
            logger.info(f'Processing file: {file} (format: {file_format})')
            
            try:
                # Load data
                df = load_files(
                    spark=spark,
                    file_formate=file_format,
                    inferSchema=file_infer_schema,
                    file_dir=file_path,
                    header=file_header
                )
                
                # Display and validate
                display_df(df, f'df_{file}')
                df_count(df, f'df_{file}')
                
                # Run ETL pipeline
                run_id = f'run_{int(time.time())}'
                logger.info(f'Starting ETL pipeline with run_id: {run_id}')
                
                metrics = run_rta_etl_on_df(df, spark, run_id)
                logger.info(f'ETL completed. Metrics: {metrics}')
                
                files_processed += 1
                
            except Exception as e:
                logger.exception(f'Failed to process {file}: {str(e)}')
                continue
        
        # Summary
        elapsed = perf_counter() - start
        logger.info('=' * 80)
        logger.info(f'Pipeline completed: {files_processed} files processed in {elapsed:.2f}s')
        logger.info('=' * 80)
        
        return 0
        
    except Exception as e:
        logger.exception(f'Fatal error in pipeline: {str(e)}')
        return 2
    finally:
        # Cleanup
        if 'spark' in locals():
            spark.stop()


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
