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
    ).withColumn("Net_sales", F.round(F.col("net_sales"), 2))
    return df1.drop("event_date")
    logging.info("Date parsing completed")
