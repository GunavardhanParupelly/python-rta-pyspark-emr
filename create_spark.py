import logging.config

from pyspark.sql import SparkSession

logging.config.fileConfig('Proporties/configeration/logging.config')

logger = logging.getLogger('create_spark')

def get_spark_object(envn, appName):
    try:
        logger.info('creating spark object..')

        if envn == 'dev':
             master = 'local'

        else:
             master = 'yarn'


        logger.info('master is {}'.format(master))


        spark = SparkSession.builder \
            .master(master) \
            .appName(appName) \
            .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .getOrCreate()

    except Exception as exp:
        logger.error('An error occured in the get_spark_object=== ', str(exp))
        raise

    else:
        logger.info('spark object created successfully..')
    return spark
