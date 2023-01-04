import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


class TableType(Enum):
    CSV = 1
    SQL = 2


class Table(ABC):
    """Abstract base class for tables."""

    @abstractmethod
    def read(self, table_type, path):
        pass

    @abstractmethod
    def write(self, table, table_type, path):
        pass


class SparkTable(Table):
    def __init__(self, spark: Union[SparkSession, SQLContext]):
        self.spark: Union[SparkSession, SQLContext] = spark

    def read(self, table_type, *args, **kwargs):
        match table_type:
            case TableType.CSV:
                return self.spark.read.csv(*args, **kwargs)
            case TableType.SQL:
                return self.spark.read.jdbc(
                    *args, **kwargs
                )
            case _:
                logging.error(f"The table type{table_type} could not be recognized")
                return None

    def write(self, df: pyspark.sql.DataFrame, table_type, *args, **kwargs):
        match table_type:
            case TableType.CSV:
                return df.write.csv(*args, **kwargs)
            case TableType.SQL:
                return df.write.jdbc(*args, **kwargs)
            case _:
                logging.error(f"The table type{table_type} could not be recognized")
                return None
