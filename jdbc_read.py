from pyspark.sql import SparkSession

spark = SparkSession.builder\
                        .appName("engineer-test")\
                        .config("spark.jars", "./postgresJDBC/postgresql-42.2.23.jar") \
                        .getOrCreate()

# spark = SparkSession.builder\
#                         .appName("engineer-test")\
#                         .config("spark.driver.extraClass", "./postgresJDBC/postgresql-42.2.23.jar") \
#                         .getOrCreate()

def ingest_pg():
    jdbcDF = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "tickets") \
    .option("user", "wellington") \
    .option("password", "Postgres7273") \
    .load()
        
    return jdbcDF

def write_pg(df):
    df.write \
    .mode("append") \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "tickets") \
    .option("user", "wellington") \
    .option("password", "Postgres7273") \
    .save()
        
    

# def ingest_pg():
#     jdbcDF = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/mydb") \
#     .option("dbtable", "tickets") \
#     .option("user", "wellington") \
#     .option("password", "Postgres7273") \
#     .load()
        
#     return jdbcDF

