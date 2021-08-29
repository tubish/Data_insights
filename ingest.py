"""functions to ingest data 
"""
from pyspark.sql import SparkSession
import logging
import configparser

config = configparser.ConfigParser()
config.read(r'configs')

url = config.get('Database', 'url')
password = config.get('Database', 'password')
username = config.get('Database', 'username')
url = config.get('Database', 'url')
dbtable = config.get('Database', 'dbtable')


def ingest_pg():
    """read a parquet data file
    """
    logging.info("Data ingestion started")
    spark = SparkSession.builder.getOrCreate()
    df = spark.read\
        .format("jdbc")\
        .option("url", url)\
        .option("dbtable", dbtable)\
        .option("user", username)\
        .option("password", password)\
        .load()

    logging.info("Data ingestion complete")
    return df


def ingest_parquet_df(fileName):
    """read a parquet data file
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
