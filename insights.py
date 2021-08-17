from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging


# 3. For each Customer, a list of Events
def list_events_for_each_customer(df):
    """For each Customer, a list of Events
    """
    logging.info("listing events for each customer")

    result = df.groupby("customer_id")\
               .agg(F.collect_list("event_code")
               .alias("List_of_events"))
    return result

# 5

def largest_Order_by_quantity_for_each_customer(df):
    """Largest Order by Quantity for each Customer
    """
    logging.info("Largest Order by Quantity for each Customer")

    result = df.groupBy("customer_id").agg(F.max("quantity"))
    return result
