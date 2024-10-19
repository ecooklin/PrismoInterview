"""Unit Tests"""

import unittest
from src.datagenerator import DataGenerator

class TestDataGenerator(unittest.TestCase):
    """Unit Tests"""
    def setUp(self):
        """Set up the DataGenerator instance before each test"""
        self.d = DataGenerator(seed=10)

    def test_basic(self):
        """Test the basic functionality of DataGenerator"""
        self.assertEqual(self.d.print_stuff(), "hello")

if __name__ == "__main__":
    unittest.main()
