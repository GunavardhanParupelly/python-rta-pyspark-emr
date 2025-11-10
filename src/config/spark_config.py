"""
Spark session configuration and creation
"""
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_object(env, app_name):
    """
    Create and configure SparkSession
    
    Args:
        env: Environment (LOCAL, DEV, PROD)
        app_name: Application name
        
    Returns:
        SparkSession object
    """
    try:
        logger.info(f'Creating Spark session for {env} environment')
        
        if env == "LOCAL":
            spark = (SparkSession.builder
                    .appName(app_name)
                    .master("local[*]")
                    .config("spark.sql.shuffle.partitions", "10")
                    .config("spark.sql.adaptive.enabled", "true")
                    .getOrCreate())
        else:
            spark = (SparkSession.builder
                    .appName(app_name)
                    .getOrCreate())
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f'Spark session created successfully: {spark.version}')
        return spark
        
    except Exception as e:
        logger.error(f'Error creating Spark session: {str(e)}')
        raise

