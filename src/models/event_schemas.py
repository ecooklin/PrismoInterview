"""Module containing the flattened event schemas"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

class EventSchemas:
    """Class to store different schemas for event types."""

    @staticmethod
    def get_account_schema() -> StructType:
        """Schema for 'account' events."""
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("domain", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("data_id", IntegerType(), True),
            StructField("data_old_status", StringType(), True),
            StructField("data_new_status", StringType(), True),
            StructField("data_reason", StringType(), True)
        ])

    @staticmethod
    def get_transaction_schema() -> StructType:
        """Schema for 'transaction' events."""
        return StructType([
            StructField("event_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("domain", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("data_id", StringType(), True),
            StructField("data_to", StringType(), True),
            StructField("data_from", StringType(), True),
            StructField("data_amount", LongType(), True),
        ])
