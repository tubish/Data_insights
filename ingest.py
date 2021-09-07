"""functions to ingest data 
"""

import configparser
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

config = configparser.ConfigParser()
config.read(r'configs')

url = config.get('Database', 'url')
password = config.get('Database', 'password')
username = config.get('Database', 'username')
url = config.get('Database', 'url')
dbtable = config.get('Database', 'dbtable')


def ingest_pg():
    """read data from postgres database
    """
    
    df = spark.read\
        .format("jdbc")\
        .option("url", url)\
        .option("dbtable", dbtable)\
        .option("user", username)\
        .option("password", password)\
        .load()

    return df


def ingest_parquet_df(fileName):
    """read a parquet data file
    """

    df = spark.read.parquet(fileName)

    return df


def ingest_csv_df(fileName):
    """ Read a csv data file 
    """

    df = spark.read.csv(fileName, header=True, inferSchema=True, sep="|")

    return df


def ingest_json_df(fileName):
    """ Read a json data file 
    """

    df = spark.read.json(fileName)

    return df
