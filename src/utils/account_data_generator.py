"""Module providing generators for the data object for the 'account' type Event class"""
from dataclasses import dataclass

from faker import Faker
from faker.providers import DynamicProvider


fake = Faker()
status_provider = DynamicProvider(
            provider_name="status",
            elements=["OPEN", "CLOSED", "PENDING", "SUSPENDED"]
        )
fake.add_provider(status_provider)

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
                "old_status": fake.status(),
                "new_status": fake.status(),
                "reason": fake.sentence()
            }
        return {}
