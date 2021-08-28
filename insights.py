"""Functions to process insights from the data
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

# 1. A table of Events with formatted dates and count of Orders


def event_table(df):
    """Aggregating orders by date
    """
    logging.info("Aggregating orders by date")

    new_df = df.groupBy("Date").count().alias("Count of orders")
    return new_df


# 2. Tickets by Customer Title, ordered by Quantity

def tickets_by_customer_title_ordered_by_quantity(df):
    """Tickets by Customer Title, ordered by Quantity
    """
    logging.info(
        "Grouping by customer title and order the data with descending quantity")

    result = df.groupBy("Customer_Title")\
               .agg(F.sum("quantity").alias("Totals"))\
               .sort("Totals", ascending=False)
    return result


# 3. For each Customer, a list of Events
def list_events_for_each_customer(df):
    """For each Customer, a list of Events
    """
    logging.info("listing events for each customer")

    result = df.groupby("customer_id")\
               .agg(F.collect_list("event_code")
                    .alias("List_of_events"))
    return result


# 4. List of **all** Customers with an additional column called "MultiEvent", set to `True` for those Customers with more than 1 Event

def customersWithMoreThanOneEvents(df):

    Tots = df.groupBy("customer_id").agg(F.count("event_name").alias("Totals"))

    return Tots.withColumn("MultiEvent", F.col("Totals") > 1)


def customersWithMoreThanOneEvents1(df):
    """using a udf 
    """

    # multiEvent = lambda x: "True" if x>1 else ""
    def multiEvent(x): return 1 if x > 1 else 0
    multiEvent_udf = udf(multiEvent, StringType())

    Tots = df.groupBy("customer_id").agg(F.count("event_name").alias("Totals"))

    return Tots.withColumn("MultiEvent", multiEvent_udf(F.col("Totals")))

# 5. Largest Order by Quantity for each Customer


def largest_Order_by_quantity_for_each_customer(df):
    """Largest Order by Quantity for each Customer
    """
    logging.info("Largest Order by Quantity for each Customer")

    result = df.groupBy("customer_id").agg(
        (F.max("quantity")).alias("MaxQuant"))
    return result


# 5.1 Largest Order by Quantity for each Customer

def largestOrderByQuantityForEachCustomer(df):
    """second method for
    """

    cols = ["customer_id", "quantity"]
    windowSpec = Window.partitionBy(["customer_id"])\
                       .orderBy(F.desc("quantity"))

    result = df.withColumn("rank", F.rank().over(windowSpec))\
               .filter("rank < 2")\
               .select(cols)

    return result.dropDuplicates()

# 6  Second largest Order by Quantity for each Customer


def secondLargestOrderByQuantityForEachCustomer(df):
    """Second largest Order by Quantity for each Customer
    """
    logging.info("Second largest Order by Quantity for each Customer")

    cols = ["customer_id", "quantity"]
    windowSpec = Window.partitionBy(["customer_id"])\
                       .orderBy(F.desc("quantity"))

    result = df.withColumn("rank", F.rank().over(windowSpec))\
               .filter("rank == 2")\
               .select(cols)

    return result.dropDuplicates()
