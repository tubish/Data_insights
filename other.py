from pyspark.sql import SparkSession

spark = SparkSession.builder\
                        .appName("engineer-test")\
                        .config("spark.driver.extraClassPath", "./postgresJDBC/postgresql-42.2.23.jar") \
                        .getOrCreate()

#df = spark.read.parquet('/mnt/c/DataSets/tickets.parquet')
df1 = spark.read.parquet('/mnt/c/DataSets/customers13.parquet')

df1.printSchema()

df1.write \
    .mode("append") \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "customers") \
    .option("user", "wellington") \
    .option("password", "Postgres7273") \
    .save()