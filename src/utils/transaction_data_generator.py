"""Module providing generators for the data object for the 'transaction' type Event class"""
from dataclasses import dataclass

from faker import Faker


fake = Faker()

@dataclass
class TransactionDataGenerator():
    """Helper Class to generate Account Event data"""
    def generate_transaction_data(self, event_type: str) -> dict:
        """Generates data payload for Transaction Events basedc on event_type"""
        if event_type == "payment-from":
            return {
                "id": fake.random_int(min=1, max=999999),
                "to": fake.random_int(min=1, max=999999),
                "from": fake.random_int(min=1, max=999999),
                "amount": fake.pyfloat(right_digits=2, min_value=0, max_value=999)
            }
        if event_type == "payment-to":
            return {
                "id": fake.random_int(min=1, max=999999),
                "to": fake.random_int(min=1, max=999999),
                "from": fake.random_int(min=1, max=999999),
                "amount": fake.pyfloat(right_digits=2, min_value=0, max_value=999)
            }
        return {}
