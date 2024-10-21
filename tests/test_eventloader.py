"""Unit Tests for the EventLoader"""

import unittest

from models.event_schemas import EventSchemas

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.eventloader import EventLoader


class TestEventLoader(unittest.TestCase):
    """Class for testing the EventLoader"""

    @classmethod
    def setUpClass(cls):
        """Setup SparkSession"""
        #cls.spark = SparkSession.builder.master("local[1]").appName("EventLoaderTests").getOrCreate()
        cls.spark = SparkSession.builder.master("local[1]").appName("EventLoaderTests").getOrCreate()
        cls.event_loader = EventLoader()

    @classmethod
    def tearDownClass(cls):
        """Stops SparkSession"""
        cls.spark.stop()

    def test_create_date_path(self):
        """Tests that a date can be transformed into the path format correctly"""
        date = "2024-10-10"
        expected = "2024/10/10/"
        result = self.event_loader.create_date_path(date)
        self.assertEqual(result, expected)

    def test_get_account_schema(self):
        """Test that the account schema is returned correctly"""
        schema = EventSchemas.get_account_schema()
        self.assertIsInstance(schema, StructType)
        expected_fields = ["event_id", "timestamp", "domain", "event_type", "data_id", "data_old_status", "data_new_status", "data_reason"]
        schema_fields = [field.name for field in schema.fields]
        self.assertEqual(schema_fields, expected_fields)

    def test_get_transaction_schema(self):
        """Test that the transaction schema is returned correctly"""
        schema = EventSchemas.get_transaction_schema()
        self.assertIsInstance(schema, StructType)
        expected_fields = ["event_id", "timestamp", "domain", "event_type", "data_id", "data_to", "data_from", "data_amount"]
        schema_fields = [field.name for field in schema.fields]
        self.assertEqual(schema_fields, expected_fields)

    def test_read_account_events(self):
        """Test reading events"""
        df = self.event_loader.read_events(path="tests/test_events.json", domain="account", event_type="account-open")
        self.assertEqual(df.count(), 1)

    def test_apply_account_schema(self):
        """test applying the account schema to events"""
        df = self.event_loader.read_events(path="tests/test_events.json", domain="account", event_type="account-open")
        schema = EventSchemas.get_account_schema()
        flat_df = self.event_loader.flatten_df(df)
        final_df = self.event_loader.cast_and_select(flat_df, schema)
        self.assertEqual(final_df.schema, schema)

    def test_read_transaction_events(self):
        """Test reading Transaction events"""
        df = self.event_loader.read_events(path="tests/test_events.json", domain="transaction", event_type="payment-to")
        self.assertEqual(df.count(), 1)

    def test_apply_transaction_schema(self):
        """test applying the Transaction schema to events"""
        df = self.event_loader.read_events(path="tests/test_events.json", domain="transaction", event_type="payment-from")
        schema = EventSchemas.get_transaction_schema()
        flat_df = self.event_loader.flatten_df(df)
        final_df = self.event_loader.cast_and_select(flat_df, schema)
        self.assertEqual(final_df.schema, schema)



if __name__ == '__main__':
    unittest.main()
