"""Event Loader Class"""
import argparse
import logging
import os
from datetime import datetime

from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import StructType

from models.event_schemas import EventSchemas
from utils.spark_utils import SparkUtils


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
        return d.strftime("%Y/%m/%d/")

    def filter_date(self, df: DataFrame, date: str, timestamp_col: str = "timestamp"):
        """Filter the input df for the given date"""
        return df.filter(to_date(col(timestamp_col)) == date)

    def get_schema(self, domain: str) -> StructType:
        """Returns the appropriate flat schema for the provided domain"""
        es = EventSchemas()
        if domain == "account":
            return es.get_account_schema()
        if domain == "transaction":
            return es.get_transaction_schema
        return StructType()

    def read_events(self, path: str, domain: str = None, event_type: str = None) -> DataFrame:
        """Reads JSON file from the provided path"""
        if event_type:
            return self.spark.read.json(path).filter((col("event_type") == event_type) & (col("domain") == domain))
        return self.spark.read.json(path)

    def write_event_dataframe(self, df: DataFrame, path: str) -> None:
        """Writes df to a provided path"""
        df.write.parquet(path=path)


if __name__ == "__main__":
    parser  = argparse.ArgumentParser()
    parser.add_argument("-s", "--source-file", type=str, help="Source Events filename", required=True)
    parser.add_argument("-t", "--target-path", type=str, help="Target path for writing parquets", default=None, required=False)
    parser.add_argument("-do", "--domain", type=str, help="Which Domain to load", required=True)
    parser.add_argument("-e", "--event-type", type=str, help="Which event type to load", required=True)
    parser.add_argument("-f", "--flatten", type=bool, help="If you want to flatten nested struct fields or not.", default=True, required=False)
    parser.add_argument("-d", "--date", type=str, help="Date to load. Expects YYYY-MM-DD representation", required=False)
    flags = parser.parse_args()
    print(flags)

    e = EventLoader()
    df = e.read_events(path=flags.source_file, domain=flags.domain, event_type=flags.event_type)
    date_df = e.filter_date(df, flags.date, "timestamp")
    if flags.flatten:
        schema = e.get_schema(flags.domain)
        flat_df = e.flatten_df(date_df)
        final_df = e.cast_and_select(flat_df, schema)
        final_df.printSchema()
        final_df.show()
    else:
        final_df = df
    if flags.target_path:
        path = os.path.join(flags.target_path, e.create_date_path(flags.date))
        e.write_event_dataframe(final_df, path=path)
