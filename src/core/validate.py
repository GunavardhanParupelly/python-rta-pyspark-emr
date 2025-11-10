import logging.config

# logging.config.fileConfig removed

loggers = logging.getLogger('validate')

def get_current_date(spark):
    try:
        loggers.warning('started the get_current_date method..')
        output = spark.sql("""select current_date""")
        print("validating spark object with current date-" + str(output.collect()))

    except Exception as e:
        loggers.error('An error occured in get_current_date method..',str(e))

        raise

    else:
        loggers.warning('validation done , go frwd...')

