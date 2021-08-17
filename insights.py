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
             
             
def largest_Order_by_quantity_for_each_customer(df):
    """Largest Order by Quantity for each Customer
    """
    logging.info("Largest Order by Quantity for each Customer")
    result = df.groupBy("customer_id").agg(F.groupBy("quantity"))
               
    return result        