"""Data Generator for Prismo Interview"""

import argparse
import json
from datetime import datetime, timedelta

from faker import Faker
from faker.providers import DynamicProvider

from models.event import Event

class DataGenerator:
    """
    Generates Data for Simulation.

    Attributes:
        seed (int): The seed to use for Faker - default 0
        num_data_points (int): Number of data points to generate
    """
    def __init__(self, seed: int, num_events: int = 10):
        self.seed = seed
        self.num_events = num_events
        self.out = []
        self.domain_provider = DynamicProvider(
            provider_name="domain",
            elements=["account", "transaction"]
        )
        self.account_provider = DynamicProvider(
            provider_name="account",
            elements=["account-open", "account-close", "status-change"]
        )
        self.transaction_provider = DynamicProvider(
            provider_name="transaction",
            elements=["payment-to", "payment-from"]
        )
        self.fake = Faker()
        Faker.seed(self.seed)
        self.fake.add_provider(self.domain_provider)
        self.fake.add_provider(self.account_provider)
        self.fake.add_provider(self.transaction_provider)

    def generate_data(self) -> None:
        """Main function to generate faked data"""
        time = datetime(2024,10,10,10,10)
        for _ in range(self.num_events):
            domain = self.fake.domain()
            if domain=="account":
                event_type=self.fake.account()
            elif domain=="transaction":
                event_type=self.fake.transaction()
            else:
                event_type=None
            e = Event(
                event_id=self.fake.uuid4(),
                timestamp=time.isoformat(),
                domain=domain,
                event_type=event_type
            )
            print(e.__dict__)
            self.out.append(e.__dict__)
            time = time + timedelta(seconds=self.fake.random_int(min=1,max=120))

    def write_data(self) -> None:
        """Writes the array of events to a file"""
        try:
            with open("events.json", "w", encoding="utf-8") as f:
                f.write("\n".join(map(json.dumps, self.out)))
        except IOError as e:
            print(f"Error writing to file: {e}")

    def print_stuff(self):
        """For testing unittest"""
        return "hello"

if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("-s", "--seed", type=int, default=0, help="Which seed to use for generating random data")
    parser.add_argument("-n", "--num_events", type=int, default=10, help="How many event to generate")
    parser.add_argument("-w", "--write_data", default=False, help="Flag to write data or not")
    flags = parser.parse_args()

    data_generator = DataGenerator(seed=flags.seed, num_events=flags.num_events)
    data_generator.generate_data()
    if flags.write_data:
        data_generator.write_data()
