from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging
# import logging.config


# logging.config.fileConfig("configs/logging.conf")


def ingest_parquet_df(fileName):
    """
    Read a parquet data file
    """
    logging.info("Data ingestion started")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.parquet(fileName)
    logging.info("Data ingestion complete")
    return df


def ingest_csv_df(fileName):
    """ 
    Read a csv data file 
    """
    logging.info("Data ingestion started")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(fileName, header=True, inferSchema=True, sep="|")
    logging.info("Data ingestion complete")
    return df


def ingest_json_df(fileName):
    """ 
    Read a json data file 
    """
    logging.info("json data ingestion started")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.json(fileName)
    logging.info("Data ingestion complete")
    return df


def parse_json_df(df):
    """
    Explode the 
    """
    logging.info("transform the file")

    df1 = df.withColumn('phone', F.col('contact_details.phone')) \
            .withColumn('postcode', F.col('contact_details.postcode')) \
            .withColumn('Customer_Title', F.col('Customer Title')) \
            .drop('contact_details')\
            .drop('Customer Title')

    return df1


def transform(df):
    logging.info("transform the data")
    pass


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

# A table of Events with formatted dates and count of Orders


def Event_table(df):
    """
    [summary]
    """
    logging.info("Aggregating orders by date")
    return df.groupBy("Date").count().alias("Count of orders")


def enrich_tickets_with_customer_details(df1, df2):
    """Combine the tickets and customers dataframes
    """
    logging.info("Enrich tickets with customer details")

    joinCondition = (df1.customer_id == df2.CustomerIdentity)
    return df1.join(F.broadcast(df2), joinCondition,
                   'inner').drop("CustomerIdentity")



def tickets_by_customer_title_ordered_by_quantity(df):
    """Tickets by Customer Title, ordered by Quantity
    """
    logging.info("Grouping by customer title and order the data with descending quantity")
    
    return df.groupBy("Customer_Title")\
             .agg(F.sum("quantity")\
             .alias("Totals"))\
             .sort("Totals", ascending=False)


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
    return df.groupBy("customer_id")\
        .agg()  
        
        
def name(args):
 pass

         