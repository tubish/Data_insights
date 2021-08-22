"""Two data sets customers and tickets are joined together with inner join of
and the date is formatted and the net_sales are rounded off to 2 decimal places.
customer_id column is dropped
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging


def parse_date(df):
    """
    Transform the date from string to date format
    """

    logging.info("Date parsing commenced")

    spark = SparkSession.builder.getOrCreate()
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    
    import pyspark.sql.functions as F

    format1 = "dd-MMM-yyyy"
    format2 = "MMM-dd-yyyy"
    format3 = "dd-MMM-yyyy"
    df1 = df.withColumn(
        "Date",
        F.when(F.to_date(F.col("event_date"), format1).isNotNull(), F.to_date(F.col("event_date"), format1),
               ).otherwise(
            F.when(F.to_date(F.col("event_date"), format2).isNotNull(), F.to_date(F.col("event_date"), format2),
                   ).otherwise(
                F.when(F.to_date(F.col("event_date"), format3).isNotNull(), F.to_date(F.col("event_date"), format3),
                       )
            ),
        ),
    ).withColumn("Net_sales", F.round(F.col("net_sales"), 2))\
     .drop("event_date")
    return df1

    logging.info("Date parsing completed")
    
def parse_json_df(df):
    """Explode the 
    """
    logging.info("transform the file")

    df1 = df.withColumn('phone', F.col('contact_details.phone')) \
            .withColumn('postcode', F.col('contact_details.postcode')) \
            .withColumn('Customer_Title', F.col('Customer Title')) \
            .drop('contact_details')\
            .drop('Customer Title')

    return df1
    
    
def enrich_tickets_with_customer_details(df1, df2):
    """Combine the tickets and customers dataframes
    """
    logging.info("Enrich tickets with customer details")

    joinCondition = (df1.customer_id == df2.CustomerIdentity)
    result = df1.join(F.broadcast(df2), joinCondition, 'inner')\
                .drop("CustomerIdentity")
    return result
