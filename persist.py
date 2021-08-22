"""data persisting methods for
"""

import logging


def persist_parquet_df(df, path):
    """ Save the data frame as parquet 
    """
    logging.info("Saving data as parquet format started")

    try:
        df.write.mode('overwrite').parquet(path)

    except Exception as exp:
        logging.error("An error ocurred while persisting data"+str(exp))

    logging.info("Saving the data as parquet format completed")


def persist_csv(df, path):
    """ Save the data frame as csv 
    """
    logging.info("Saving the data as csv format started")

    try:
        df.coalesce(1).write.mode('overwrite').csv(path)

    except Exception as exp:
        logging.error("An error ocurred while persisting data"+str(exp))

    logging.info("Saving the data as csv format completed")
