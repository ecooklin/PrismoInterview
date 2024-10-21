"""Unit Tests for the Data Generators"""

import unittest
from src.datagenerator import DataGenerator
from src.utils.account_data_generator import AccountDataGenerator
from src.utils.transaction_data_generator import TransactionDataGenerator

class TestDataGenerator(unittest.TestCase):
    """Unit Tests"""
    def setUp(self):
        """Set up the various generators before each test"""
        self.data_gen = DataGenerator(seed=10, base_path="data/raw_events/",start_date="2024-10-10", num_events=10)
        self.account_gen = AccountDataGenerator()
        self.transaction_gen = TransactionDataGenerator()

    def test_generate_events(self):
        """Test basic event generation"""
        self.data_gen.generate_data()
        self.assertEqual(len(self.data_gen.out), 10)

    def test_generate_account_data(self):
        """Test the generation of the data object for account domain events"""

        # Test for account-open
        data = self.account_gen.generate_account_data("account-open")
        self.assertIn("id", data)
        self.assertEqual(data["new_status"], "PENDING") # New accounts should default to PENDING status

        # Test for account-close
        data = self.account_gen.generate_account_data("account-close")
        self.assertIn("id", data)
        self.assertIsNone(data["old_status"])           # I dont have a good way of generating an account's last status :(
        self.assertEqual(data["new_status"], "CLOSED")  # new status should always be closed

        # Test for status-change
        data = self.account_gen.generate_account_data("status-change")
        self.assertIn("id", data)
        self.assertIn(data["old_status"], ["OPEN", "CLOSED", "PENDING", "SUSPENDED"])  # Should be one of the defined statuses
        self.assertIn(data["new_status"], ["OPEN", "CLOSED", "PENDING", "SUSPENDED"])  # Should be one of the defined statuses
        self.assertNotEqual(data["old_status"], data["new_status"])                    # Old and new status should be different

    def test_generate_transaction_data(self):
        """Test the generation of the data object for transaction domain events"""
        data = self.transaction_gen.generate_transaction_data("payment-to")
        self.assertIn("id", data)
        self.assertIn("amount", data)


if __name__ == "__main__":
    unittest.main()
