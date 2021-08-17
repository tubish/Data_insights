from pyspark.sql import SparkSession
from utils import *
from insights import *
from date_formatting import parse_date
import logging


tickets_csv_path = "./data/tickets.csv"
customers_json_path_path = "./data/customers.json"

tickets_path = "./output/tickets.parquet"
customers_path = "./output/customers.parquet"
processed_df_path = "./output/processed_df.parquet"

FORMAT='%(asctime)s:%(name)s:%(message)s'
logging.basicConfig(filename="test.log", level="INFO",format=FORMAT)


def main():
    """[summary]
    """
    
    logging.info("Starting the pipeline")
    
    spark = SparkSession.builder.appName("engineer-test").getOrCreate()

    tickets_df = ingest_csv_df(tickets_csv_path)
    tickets = parse_date(tickets_df)
    persist_parquet_df(tickets, tickets_path)

    customers_df = ingest_json_df(customers_json_path_path)
    customers = parse_json_df(customers_df)
    persist_parquet_df(customers, customers_path)

    tickets = ingest_parquet_df(tickets_path)
    customers = ingest_parquet_df(customers_path)

    df = enrich_tickets_with_customer_details(tickets, customers)
    df.show()
    persist_parquet_df(df, processed_df_path)

    # 1. A table of Events with formatted dates and count of Orders

    # 2. Tickets by Customer Title, ordered by Quantity
    tickets_by_customer_title_ordered_by_quantity(df).show()
    
    # 3. For each Customer, a list of Events
    list_events_for_each_customer(df).show(5, truncate=False)
    
    # 4. List of **all** Customers with an additional column called "MultiEvent", set to `True` for those Customers with more than 1 Event
    
    # 5. Largest Order by Quantity for each Customer
    largest_Order_by_quantity_for_each_customer(df).show(5)
    


if __name__ == "__main__":
    main()
