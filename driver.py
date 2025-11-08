import logging
import logging.config
import os
import sys
from time import perf_counter
import time

if sys.platform.startswith('win'):
    hadoop_home = r'C:\spark\hadoop-3.3.5'
    os.environ.setdefault('HADOOP_HOME', hadoop_home)
    bin_path = os.path.join(hadoop_home, 'bin')
    if bin_path not in os.environ.get('PATH', ''):
        os.environ['PATH'] = os.environ['PATH'] + ';' + bin_path
    # Ensure JAVA_HOME (adjust path)
    os.environ.setdefault('JAVA_HOME', r'C:\Program Files\Java\jdk-17')

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(ROOT, 'src')
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

import get_env_variables as gav
from create_spark import get_spark_object
from ingest import load_files, display_df, df_count
from transformation import run_rta_etl_on_df
from validate import get_current_date

# Logging config existence check
_log_cfg = 'Properties/configuration/logging.config'
if not os.path.isfile(_log_cfg):
    print(f'WARN: Logging config not found: {_log_cfg}')
else:
    logging.config.fileConfig(_log_cfg)
logging.getLogger().setLevel(logging.INFO)

def main():
    start = perf_counter()
    try:
        logging.info('Starting main')
        spark = get_spark_object(gav.envn, gav.appName)
        get_current_date(spark)

        src_dir = gav.src_olap
        if not os.path.isdir(src_dir):
            logging.error('Source dir missing: %s', src_dir)
            return 1

        for fname in os.listdir(src_dir):
            path = os.path.join(src_dir, fname)
            if not os.path.isfile(path):
                continue

            if fname.endswith('.parquet'):
                file_format = 'parquet'
                header = None
                infer_schema = None
            elif fname.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                infer_schema = gav.inferSchema
            else:
                continue

            logging.info('Reading %s (%s)', fname, file_format)
            try:
                df = load_files(
                    spark=spark,
                    file_formate=file_format,  # keep legacy param name
                    inferSchema=infer_schema,
                    file_dir=path,
                    header=header
                )
            except Exception:
                logging.exception('Load failed for %s', fname)
                continue

            display_df(df, 'df_transport')
            df_count(df, 'df_transport')

            try:
                run_id = f'run_{int(time.time())}'
                metrics = run_rta_etl_on_df(
                    df,
                    spark,
                    output_root=gav.trans_path,
                    mode='overwrite',
                    run_id=run_id,
                    output_format='csv'
                )
                logging.info('Metrics %s: %s', fname, metrics)
            except Exception:
                logging.exception('ETL failed for %s', fname)

        logging.info('Elapsed %.2fs', perf_counter() - start)
        return 0
    except Exception:
        logging.exception('Fatal error')
        return 2

if __name__ == '__main__':
    rc = main()
    logging.info('Done exit=%s', rc)
    sys.exit(rc)
