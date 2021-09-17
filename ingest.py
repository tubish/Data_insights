"""functions to ingest data 
"""

from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read(r'configs')

url = config.get('Database', 'url')
password = config.get('Database', 'password')
username = config.get('Database', 'username')
dbtable = config.get('Database', 'dbtable')
jar_path = config.get('JARS', 'jar_path')

def run_spark():
    spark = SparkSession\
                .builder\
                .config('spark.jars', './postgresJDBC/postgresql-42.2.23.jar') \
                .getOrCreate()
        
    return spark

spark = run_spark()


def spark_jdbc_read(spark):
    jdbcDF = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "tourneys") \
    .option("user", "wellington") \
    .option("password", "Postgres7273") \
    .load()
        
    return jdbcDF


def ingest_parquet_df(fileName):
    """read a parquet data file"""
    return spark.read.parquet(fileName)


def ingest_csv_df(fileName):
    """ Read a csv data file """
    return spark.read.csv(fileName, header=True, inferSchema=True, sep="|")


def ingest_json_df(fileName):
    """ Read a json data file """
    return spark.read.json(fileName)

