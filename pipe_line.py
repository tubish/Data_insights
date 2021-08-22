from pyspark.sql import SparkSession
from utils import *
from insights import *
from transform_data import enrich_tickets_with_customer_details, parse_date
import logging
import configparser


FORMAT = '%(asctime)s:%(name)s:%(message)s'
logging.basicConfig(filename="test.log", level="INFO", format=FORMAT)


def main():
    """run the pipeline
    """

    logging.info("Starting the pipeline")

    spark = SparkSession.builder.appName("engineer-test").getOrCreate()

    config = configparser.ConfigParser()
    config.read(r'configs/conf.ini')

    tickets_csv_path = config.get('paths', 'tickets_csv_path')
    customers_json_path_path = config.get('paths', 'customers_json_path_path')
    tickets_path = config.get('paths', 'tickets_path')
    customers_path = config.get('paths', 'customers_path')
    processed_df_path = config.get('paths', 'processed_df_path')
    largest_oder_path = config.get('paths', "largest_oder_path")

    tickets = ingest_parquet_df(tickets_path)
    customers = ingest_parquet_df(customers_path)

    # 1. A table of Events with formatted dates and count of Orders
    df = enrich_tickets_with_customer_details(tickets, customers)
    df.show(5)
    print(df.count())
    persist_parquet_df(df, processed_df_path)

    # 2. Tickets by Customer Title, ordered by Quantity
    tickets_by_customer_title_ordered_by_quantity(df).show()

    # 3. For each Customer, a list of Events
    list_events_for_each_customer(df).show(5, truncate=False)

    # 4. List of **all** Customers with an additional column called "MultiEvent", set to `True` for those Customers with more than 1 Event

    # 5. Largest Order by Quantity for each Customer
    largest_Order_by_quantity_for_each_customer(df).show(5)
    largestOrderByQuantityForEachCustomer(df).show(5)
    
    # 7. Gap/Delta in Quantity between each Customers Order
    
    # 8. A Spark SQL Table containing all provided data in a denormalized structure, ordered by Event date
    
    # 9. Create a Data Model, with a Transaction, Customer and Event table, providing a way to join the 3 tables
    
    #10. Net Sales by Season, with percentage comparison to the previous Season
    
    #11. Find the latest event purchased by customer, 
    # and depending on what season the event date falls in 
    # assign the status of 'Current Ticket Purchaser', 'Previous Ticket Purchaser' or 'Future Ticket Purchaser'.

if __name__ == "__main__":
    main()
