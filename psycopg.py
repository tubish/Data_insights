# https://www.youtube.com/watch?v=SM8YqCy2W8o solution to installing pyscopg2 problem


from pyspark.sql import SparkSession
import psycopg2
import pandas as pd
import pandas.io.sql as sqlio

import logging

spark = SparkSession.builder.getOrCreate()

def ingest_pg_psy():
    """read data from postgres database
    """
    logging.info("psycopg is ingesting from database")
    connection = psycopg2.connect(host='localhost', port=5432, password='Postgres7273', database='mydb')
    cursor = connection.cursor()
    sql_query = """SELECT * from tickets"""
    pdf = sqlio.read_sql_query(sql_query, connection)
    sparkDF = spark.createDataFrame(pdf)
    
    return sparkDF
ingest_pg_psy().show()
