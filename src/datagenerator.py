"""Data Generator for Prismo Interview"""

import argparse
import json
import logging
import os
from datetime import datetime, timedelta

from faker import Faker
from faker.providers import DynamicProvider

from models.event import Event


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


class DataGenerator:
    """
    Generates Data for Simulation.

    Attributes:
        seed (int): The seed to use for Faker - default 0
    """
    def __init__(self, seed: int):
        self.seed = seed
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

    def generate_data(self, start_date: str, num_events: int) -> None:
        """
        Main function to generate faked data

        Args:
            start_date (str): Which date to start generating from
            num_events (int): How many data points to generate
        """
        logger.info("Generating data")
        time = datetime.strptime(start_date, "%Y-%m-%d").date()
        for _ in range(num_events):
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
            logger.debug(e.__dict__)
            self.out.append(e.__dict__)
            time = time + timedelta(seconds=self.fake.random_int(min=1,max=120))

    def write_data(self, base_path: str) -> None:
        """Checks for the base path and writes the array of events to a file"""
        logger.info("Writing Data")
        logger.debug(base_path)
        try:
            os.makedirs(os.path.dirname(base_path), exist_ok=True)
            file = os.path.join(base_path, "events.json")
            with open(file, mode="w", encoding="utf-8") as f:
                f.write("\n".join(map(json.dumps, self.out)))
        except IOError as e:
            logger.error("Error writing to file: %s", e)


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("-s", "--seed", type=int, default=0, help="Which seed to use for generating random data", required=False)
    parser.add_argument("-n", "--num-events", type=int, default=10, help="How many event to generate", required=False)
    parser.add_argument("-d", "--start-date", type=str, default="2024-10-10", help="Which date to start generating events from. Expects YYYY-MM-DD format.", required=False)
    parser.add_argument("-w", "--write-data", action="store_true", default=False, help="Include to write data to  the base-path", required=False)
    parser.add_argument("-b", "--base-path", type=str, default="data/raw_events/", help="Base path to write data to. Requires --write-data", required=False)
    flags = parser.parse_args()
    logger.debug(flags)

    data_generator = DataGenerator(seed=flags.seed)
    data_generator.generate_data(flags.start_date, flags.num_events)

    logger.debug(flags.write_data, type(flags.write_data))
    if flags.write_data:
        data_generator.write_data(flags.base_path)
