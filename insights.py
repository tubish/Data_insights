from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging


def list_events_for_each_customer(df):
    """For each Customer, a list of Events
    """
    logging.info("listing events for each customer")
    
    return df.groupby("customer_id")\
             .agg(F.collect_list("event_name")\
             .alias("List_of_events"))