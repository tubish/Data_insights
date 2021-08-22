from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
# import logging.config


# logging.config.fileConfig("configs/logging.conf")


def ingest_parquet_df(fileName):
    """Read a parquet data file
    """
    logging.info("Data ingestion started")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(fileName)
    logging.info("Data ingestion complete")
    return df


def ingest_csv_df(fileName):
    """ Read a csv data file 
    """
    logging.info("Data ingestion started")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(fileName, header=True, inferSchema=True, sep="|")
    logging.info("Data ingestion complete")
    return df


def ingest_json_df(fileName):
    """ Read a json data file 
    """
    logging.info("json data ingestion started")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.json(fileName)
    logging.info("Data ingestion complete")
    return df





def persist_parquet_df(df, path):
    """ 
    Save the data frame as parquet 
    """
    logging.info("Saving the data frame as parquet format started")

    try:
        df.write.mode('overwrite').parquet(path)

    except Exception as exp:
        logging.error("An error ocurred while persisting data"+str(exp))

    logging.info("Saving the data as parquet format completed")


def persist_csv(df, path):
    """ 
    Save the data frame as parquet 
    """
    logging.info("Saving the data frame as parquet format started")

    try:
        df.coalesce(1).write.mode('overwrite').csv(path)

    except Exception as exp:
        logging.error("An error ocurred while persisting data"+str(exp))

    logging.info("Saving the data as parquet format completed")





# def list_events_for_each_customer(df):
#     """For each Customer, a list of Events
#     """
#     logging.info("listing events for each customer")

#     return df.groupby("customer_id")\
#              .agg(F.collect_list("event_name")\
#              .alias("List_of_events"))
