"""data persisting methods 
"""

import logging


def persist_parquet_df(df, path):
    """ Save the data frame as parquet 
    """

    try:
        df.write.mode('overwrite').parquet(path)

    except Exception as exp:
        logging.error("An error ocurred while persisting data"+str(exp))



def persist_csv(df, path):
    """ Save the data frame as csv 
    """

    try:
        df.coalesce(1).write.mode('overwrite').csv(path)

    except Exception as exp:
        logging.error("An error ocurred while persisting data"+str(exp))

    
