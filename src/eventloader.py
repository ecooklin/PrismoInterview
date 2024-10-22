"""Event Loader Class"""
import argparse
import logging
import os
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType

from models.event_schemas import EventSchemas
from utils.spark_utils import SparkUtils


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


class EventLoader(SparkUtils):
    """Event Loader Class"""
    def __init__(self):
        self.spark = SparkSession.builder \
                        .appName("Prismo Event Loader") \
                        .getOrCreate()
        self.spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    def create_date_path(self, date: str):
        """Creates date path structure like year/month/date based on target dates"""
        d = datetime.strptime(date, "%Y-%m-%d")
        logger.debug(d.strftime("%Y/%m/%d/"))
        return d.strftime("%Y/%m/%d/")

    def filter_date(self, df: DataFrame, date: str, timestamp_col: str = "timestamp"):
        """Filter the input df for the given date"""
        return df.filter(to_date(col(timestamp_col)) == date)

    def get_distinct_dates(self, df: DataFrame) -> List[str]:
        """Grabs all of the distinct dates in df"""
        return df.select(to_date(col("timestamp")).alias("date")).distinct().collect()

    def get_schema(self, domain: str) -> StructType:
        """Returns the appropriate flat schema for the provided domain"""
        es = EventSchemas()
        logger.debug(domain)
        if domain == "account":
            return es.get_account_schema()
        if domain == "transaction":
            return es.get_transaction_schema()
        return StructType()

    def read_events(self, path: str, domain: str = None, event_type: str = None) -> DataFrame:
        """Reads JSON file from the provided path and returns a DataFrame filtered by domain and event_type"""
        logger.info("Reading data from path: %s", path)
        if event_type:
            return self.spark.read.json(path).filter((col("event_type") == event_type) & (col("domain") == domain))
        return self.spark.read.json(path)

    def write_event_dataframe(self, df: DataFrame, path: str) -> None:
        """Writes df to a provided path"""
        logger.info("Writing parquet data to: %s", path)
        df.write.mode("overwrite").parquet(path=path)

    def load_events(self,
                    df: DataFrame,
                    event_type: str,
                    date: str,
                    schema: StructType,
                    base_path: str
                    ) -> None:
        """
        Loads events to the path

        Args:
            df (DataFrame): Input DataFrame
            date (str): Date string YYYY-MM-DD format
            schema (str): The flattened df schema
            base_path (str): Base path to write to. 
        """
        date_df = self.filter_date(df, date=date, timestamp_col="timestamp")
        flat_df = self.flatten_df(date_df)
        cast_df = self.cast_and_select(flat_df, schema)
        final_df = self.dedupe_events(cast_df)
        date_path = os.path.join(base_path, self.create_date_path(date), event_type)
        self.write_event_dataframe(final_df, date_path)

    def backfill_events(self,
                        df: DataFrame,
                        event_type: str,
                        dates: List[str],
                        schema: StructType,
                        base_path: str
                        ) -> None:
        """
        Takes in a super df of event data, flattens it, applies a schema, and writes it partitioned by date to the provided path.

        Args:
            df (DataFrame): Input DataFrame
            date (str): Date string YYYY-MM-DD format
            schema (str): The flattened df schema
            base_path (str): Base path to write to. 
        """
        for date in dates:
            date_str = date["date"].strftime("%Y-%m-%d")
            logger.info(date_str)
            date_df = self.filter_date(df, date=date_str, timestamp_col="timestamp")
            flat_df = self.flatten_df(date_df)
            cast_df = self.cast_and_select(flat_df, schema)
            final_df = self.dedupe_events(cast_df)
            date_path = os.path.join(base_path, self.create_date_path(date_str), event_type)
            self.write_event_dataframe(final_df, date_path)


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("-s", "--source-file", type=str, help="Source Events filename", required=True)
    parser.add_argument("-t", "--target-path", type=str, help="Target path for writing parquets", default=None, required=True)
    parser.add_argument("-do", "--domain", type=str, help="Which Domain to load", required=True)
    parser.add_argument("-e", "--event-type", type=str, help="Which event type to load", required=True)
    parser.add_argument("-f", "--flatten", type=bool, help="If you want to flatten nested struct fields or not.", default=True, required=False)
    parser.add_argument("-d", "--date", type=str, help="Date to load. Expects YYYY-MM-DD representation", required=False)
    parser.add_argument("-b", "--backfill", action="store_true", help="Backfill ALL events from the source-file and overwrite the data in target-path", required=False)
    flags = parser.parse_args()
    logger.debug(flags)

    e = EventLoader()
    df = e.read_events(path=flags.source_file, domain=flags.domain, event_type=flags.event_type)
    schema = e.get_schema(flags.domain)
    logger.debug(schema)

    if flags.backfill:
        logger.info("Backfilling Event Data for evemt type: %s", flags.event_type)
        dates = e.get_distinct_dates(df)
        logger.info("Full dates list %s", dates)
        e.backfill_events(df, flags.event_type, dates, schema, flags.target_path)
    else:
        logger.info("Loading event data")
        e.load_events(df, flags.event_type, flags.date, schema, flags.target_path)
