"""Module providing the base Event Class."""

from utils.account_data_generator import AccountDataGenerator
from utils.transaction_data_generator import TransactionDataGenerator

class Event(
    AccountDataGenerator,
    TransactionDataGenerator
):
    "Class representing an Event"
    def __init__(self,
                 event_id: str = None,
                 timestamp: str = None,
                 domain: str = None,
                 event_type: str = None,
                 ):
        self.event_id = event_id
        self.timestamp = timestamp
        self.domain = domain
        self.event_type = event_type
        if self.domain == "account":
            self.data = self.generate_account_data(self.event_type)
        elif self.domain == "transaction":
            self.data = self.generate_transaction_data(self.event_type)
        else:
            self.data = {}
