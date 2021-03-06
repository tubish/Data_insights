"""
Two data sets customers and tickets are joined together with inner join of
and the date is formatted and the net_sales are rounded off to 2 decimal places.
customer_id column is dropped
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.getOrCreate()
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


def parse_date(df):
    """
    Transform the date from string to date format and round net_sales to 2 decimal places
    """

    format1 = "dd-MMM-yyyy"
    format2 = "MMM-dd-yyyy"
    format3 = "dd-MMM-yyyy"
    return df.withColumn(
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


def parse_json_df(df):
    """Explode the 
    """

    return df.withColumn('phone', F.col('contact_details.phone')) \
             .withColumn('postcode', F.col('contact_details.postcode')) \
             .withColumn('Customer_Title', F.col('Customer Title')) \
             .drop('contact_details')\
             .drop('Customer Title')


def enrich_tickets_with_customer_details(df1, df2):
    """Combine the tickets and customers dataframes
    """

    joinCondition = (df1.customer_id == df2.CustomerIdentity)
    return df1.join(F.broadcast(df2), joinCondition, 'inner')\
              .drop("CustomerIdentity")
