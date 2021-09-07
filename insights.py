"""Functions to process insights from the data
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, lag
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging



def event_table(df):
    """A table of Events with formatted dates and count of Orders
    """

    new_df = df.groupBy("Date")\
               .count()\
               .alias("Count of orders")
    
    return new_df



def tickets_by_customer_title_ordered_by_quantity(df):
    """ Tickets by Customer Title, ordered by Quantity
    """

    result = df.groupBy("Customer_Title")\
               .agg(F.sum("quantity").alias("Totals"))\
               .sort("Totals", ascending=False)
               
    return result


def list_events_for_each_customer(df):
    """For each Customer, a list of Events
    """

    result = df.groupby("customer_id")\
               .agg(F.collect_list("event_code")\
               .alias("List_of_events"))
               
    return result



def customersWithMoreThanOneEvents(df):
    """List of **all** Customers with an additional column called "MultiEvent", 
    set to `True` for those Customers with more than 1 Event
    """

    df1 = df.groupBy("customer_id").agg(F.count("event_name").alias("Totals"))\
            .withColumn("MultiEvent", F.col("Totals") > 1)

    return df1


def customersWithMoreThanOneEvents1(df):
    """using a udf 
    """

    # multiEvent = lambda x: "True" if x>1 else ""
    def multiEvent(x): return 1 if x > 1 else 0
    multiEvent_udf = udf(multiEvent, StringType())

    Tots = df.groupBy("customer_id").agg(F.count("event_name").alias("Totals"))

    return Tots.withColumn("MultiEvent", multiEvent_udf(F.col("Totals")))



def largest_Order_by_quantity_for_each_customer(df):
    """Largest Order by Quantity for each Customer
    """

    result = df.groupBy("customer_id")\
               .agg((F.max("quantity"))\
               .alias("MaxQuant"))
    return result



def largestOrderByQuantityForEachCustomer(df):
    """Largest Order by Quantity for each Customer
    """

    cols = ["customer_id", "quantity"]
    windowSpec = Window.partitionBy(["customer_id"])\
                       .orderBy(F.desc("quantity"))

    result = df.withColumn("rank", F.rank().over(windowSpec))\
               .filter("rank < 2")\
               .select(cols)

    return result.dropDuplicates()



def secondLargestOrderByQuantityForEachCustomer(df):
    """Second largest Order by Quantity for each Customer
    """

    cols = ["customer_id", "quantity"]
    windowSpec = Window.partitionBy(["customer_id"])\
                       .orderBy(F.desc("quantity"))

    result = df.withColumn("rank", F.rank().over(windowSpec))\
               .filter("rank == 2")\
               .select(cols)

    return result.dropDuplicates()



def delta_in_quantity_between_each_customers_order(df):
    """Gap/Delta in Quantity between each Customers Order
    """
    
    # cols = ["Date", "customer_id", "ticket_id", "quantity"]
    # df1 = df.select(cols)
    windowSpec = Window.partitionBy(["customer_id"])\
                        .orderBy("Date")
                        
    r = df.withColumn("lag",lag("quantity", 1).over(windowSpec)) \
        .withColumn("delta", F.round(F.col("lag")-F.col("quantity")))\
        .na.fill(value=0)
           
    return r
