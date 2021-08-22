from pyspark.sql import SparkSession
from utils import *
from insights import list_events_for_each_customer
import logging


tickets_csv_path = "./data/tickets.csv"
customers_json_path_path = "./data/customers.json"

tickets_path = "./output/tickets.parquet"
customers_path = "./output/customers.parquet"
processed_df_path = "./output/processed_df.parquet"

logging.basicConfig(filename="test.log", level="INFO",
                    format='%(asctime)s:%(name)s:%(message)s')


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
    
    # 7. Gap/Delta in Quantity between each Customers Order
    
    # 8. A Spark SQL Table containing all provided data in a denormalized structure, ordered by Event date
    
    # 9. Create a Data Model, with a Transaction, Customer and Event table, providing a way to join the 3 tables
    
    #10. Net Sales by Season, with percentage comparison to the previous Season
    
    #11. Find the latest event purchased by customer, 
    # and depending on what season the event date falls in 
    # assign the status of 'Current Ticket Purchaser', 'Previous Ticket Purchaser' or 'Future Ticket Purchaser'.

if __name__ == "__main__":
    main()
