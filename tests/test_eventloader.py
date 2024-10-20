"""Unit Tests for the EventLoader"""

import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from src.eventloader import EventLoader


class TestEventLoader(unittest.TestCase):
    """Class for testing the EventLoader"""

    @classmethod
    def setUpClass(cls):
        """Setup SparkSession"""
        cls.spark = SparkSession.builder.master("local[1]").appName("EventLoaderTests").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stops SparkSession"""
        cls.spark.stop()



if __name__ == '__main__':
    unittest.main()
