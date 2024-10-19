"""Module providing generators for the data object for the 'account' type Event class"""
from dataclasses import dataclass

from faker import Faker


fake = Faker()

@dataclass
class AccountDataGenerator():
    """Helper Class to generate Account Event data"""
    def generate_account_data(self, event_type: str) -> dict:
        """Generates data payload for Account Events basedc on event_type"""
        if event_type == "account-open":
            return {
                "id": fake.random_int(min=1, max=999999),
                "old_status": None,
                "new_status": "PENDING",
                "reason": fake.sentence()
            }
        if event_type == "account-close":
            return {
                "id": fake.random_int(min=1, max=999999),
                "old_status": None,
                "new_status": "CLOSED",
                "reason": fake.sentence()
            }
        if event_type == "status-change":
            return {
                "id": fake.random_int(min=1, max=999999),
                "old_status": "TEST",
                "new_status": "PENDING",
                "reason": fake.sentence()
            }
        return {}
