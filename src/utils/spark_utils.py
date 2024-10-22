"""Common Utility Class containing functions for transforming Spark DataFrames"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

@dataclass
class SparkUtils:
    """Utils for working on spark dataframes. (Imagined as a mixin that would be used across spark projects)"""
    def flatten_df(self, df: DataFrame) -> None:
        """
        Recursively flattens a dataframe with multiple levels of nested structures.

        Args:
            df (DataFrame): The input dataframe
        
        Returns:
            DataFrame: Flattened dataframe
        """
        def flatten_struct_fields(inner_df: DataFrame) -> DataFrame:
            flat_cols = []
            nested_cols = []

            for field in inner_df.schema.fields:
                if isinstance(field.dataType, StructType):
                    for nested_field in field.dataType.fields:
                        nested_cols.append(col(f"{field.name}.{nested_field.name}")
                                        .alias(f"{field.name}_{nested_field.name}"))
                else:
                    flat_cols.append(col(field.name))

            return flat_cols + nested_cols

        # Recursively flatten the DataFrame until there are no more struct fields
        while any(isinstance(field.dataType, StructType) for field in df.schema.fields):
            df = df.select(flatten_struct_fields(df))
        return df

    def cast_and_select(self, df: DataFrame, schema: StructType) -> DataFrame:
        """
        Casts the columns in the provided dataframe based on the schema. Returns only the columns in the schema.
        
        Args:
            df (DataFrame): The input DataFrame containing all event types.
            schema (StructType): The schema to cast the filtered DataFrame to.
        
        Returns:
            DataFrame: Filtered and cast DataFrame with only the necessary columns.
        """
        schema_cols = [field.name for field in schema.fields]

        for field in schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))
        return df.select(*schema_cols)

    def dedupe_events(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate events by keeping the latest event for each event_id.

        Args:
            df (DataFrame): Input DataFrame containing events.

        Returns:
            DataFrame: Deduplicated DataFrame containing only the latest events per event_id.
        """
        window_spec = Window.partitionBy("event_id").orderBy(desc("timestamp"))
        rn_df = df.withColumn("row_number", row_number().over(window_spec))
        dedupe_df = rn_df.filter(col("row_number") == 1).drop("row_number")

        return dedupe_df
