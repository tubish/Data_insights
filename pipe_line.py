from pyspark.sql import SparkSession
from ingest import *
from insights import *
from persist import *
from transform_data import enrich_tickets_with_customer_details, parse_date
import logging
import configparser
import os

filename = './test.log'
if os.path.exists(filename):
    os.remove(filename)

FORMAT = '%(asctime)s:%(name)s:%(message)s'
#logging.basicConfig(filename="test.log", level="ERROR", format=FORMAT)
logging.basicConfig(level="INFO", format=FORMAT)


def main():
    """run the pipeline
    """

    logging.info("Starting the pipeline")
    
    config = configparser.ConfigParser()
    config.read(r'configs/conf.ini')
    
    tickets_csv_path = config.get('paths', 'tickets_csv_path')
    tickets_csv_path = config.get('paths', 'tickets_csv_path')
    customers_json_path_path = config.get('paths', 'customers_json_path_path')
    tickets_path = config.get('paths', 'tickets_path')
    customers_path = config.get('paths', 'customers_path')
    processed_df_path = config.get('paths', 'processed_df_path')
    jar_path = config.get('jars', 'jar_path')

    spark = SparkSession.builder\
        .appName("engineer-test")\
        .getOrCreate()
        

    config = configparser.ConfigParser()
    config.read(r'configs/conf.ini')

    # dff = ingest_pg()
    # dff.show()


    tickets = ingest_parquet_df(tickets_path)
    customers = ingest_parquet_df(customers_path)

    df = enrich_tickets_with_customer_details(tickets, customers)
    df.cache()
    # df.show(5)
    # print(df.count())
    # persist_parquet_df(df, processed_df_path)

    # 1. A table of Events with formatted dates and count of Orders
    event_table(df).show()

    # 2. Tickets by Customer Title, ordered by Quantity
    tickets_by_customer_title_ordered_by_quantity(df).show()

    # 3. For each Customer, a list of Events
    list_events_for_each_customer(df).show(5, truncate=False)

    # 4. List of **all** Customers with an additional column called "MultiEvent", set to `True` for those Customers with more than 1 Event
    customersWithMoreThanOneEvents(df).show(10)
    customersWithMoreThanOneEvents1(df).show(10)

    # 5. Largest Order by Quantity for each Customer
    largest_Order_by_quantity_for_each_customer(df).show(5)
    largestOrderByQuantityForEachCustomer(df).show(5)

    # 6. Second Largest order by Quantity for each Customer
    secondLargestOrderByQuantityForEachCustomer(df).show(5)

    # 7. Gap/Delta in Quantity between each Customers Order

    # 8. A Spark SQL Table containing all provided data in a denormalized structure, ordered by Event date

    # 9. Create a Data Model, with a Transaction, Customer and Event table, providing a way to join the 3 tables

    # 10. Net Sales by Season, with percentage comparison to the previous Season

    # 11. Find the latest event purchased by customer,
    # and depending on what season the event date falls in
    # assign the status of 'Current Ticket Purchaser', 'Previous Ticket Purchaser' or 'Future Ticket Purchaser'.

    logging.info('application is finished')
    spark.stop()
    return None


if __name__ == "__main__":
    main()
