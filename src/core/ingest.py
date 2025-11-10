import logging.config

# logging.config.fileConfig removed

loggers = logging.getLogger('ingest')

def load_files(spark, file_formate, inferSchema, header, file_dir=None):
    global df
    try:
        loggers.warning('load_files method stared...')

        if file_formate == 'parquet':
            df = spark.read.format(file_formate).load(file_dir)

        elif file_formate == 'csv':
            df = spark.read.format(file_formate).option("header", header).option("inferSchema" , inferSchema).load(file_dir)

    except Exception as e:
        loggers.error('An error occurred at loading_file=== %s',str(e))
        raise
    else:
        loggers.warning ('dataframe created sucessfully which is of {}'.format(file_formate))
        return df

def display_df(df: object, dfName: object) -> object:
    loggers.info(f"Displaying DataFrame: {dfName}")
    df_show = df.show()
    return df_show


def df_count(df, dfName):
    try:
        loggers.warning('here to count the records in the {}'.format(dfName))

        df_c = df.count()

    except Exception as exp:
        raise
    else:
        loggers.warning("Number of records present in the {} are :: {}".format(df, df_c))

    return df_c

