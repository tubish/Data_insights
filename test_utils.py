import unittest
from utils import *
from pipe_line import *


class utils(unittest.TestCase):

    def test_enrich_tickets_with_customer_details(self):
        """Test data transformer.
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        tickets = ingest_parquet_df(tickets_path)
        customers = ingest_parquet_df(customers_path)
        enriched_df = enrich_tickets_with_customer_details(tickets, customers)
        
        # check number of rows and columns
        expected_columns = len(enriched_df.columns)
        expected_rows = enriched_df.count()
        
        # check for null values in Date column
        count_nulls_in_Date_column = tickets.select("Date").where(F.col("Date").isNull()).count()

        
        self.assertEqual(expected_columns, 14)
        self.assertEqual(expected_rows, 1000)
        self.assertEqual(count_nulls_in_Date_column, 0)


if __name__ == "__main__":
    unittest.main()
