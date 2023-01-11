import logging

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


class SparkHandler:
    def __init__(self, spark: SparkSession, db_url: str, user_name: str, password: str, database_name: str):
        """

        Args:
            spark (pyspark.sql.session.SparkSession):
            db_url (str):
            user_name (str):
            password (str):
            database_name (str):
        """
        self.__spark = spark
        self.__password = password
        self.__user_name = user_name
        self.__database_name = database_name
        self.__db_url = db_url

    @property
    def spark(self) -> SparkSession:
        """
        Getter method for spark
        Returns:
            SparkSession: class spark variable
        """
        return self.__spark

    def read_table(self, table_name: str) -> pyspark.sql.dataframe.DataFrame:
        """
        Function for reading a table and loading it
        Args:
            table_name (str): The name of the table

        Returns:
            pyspark.sql.dataframe.DataFrame: loaded table
        """
        # TODO change to more secure way of connecting. e.g. using ssh keys ...
        try:
            return self.spark.read.format("jdbc") \
                .option("url", f"{self.__db_url}{self.__database_name}") \
                .option("dbtable", table_name) \
                .option("user", self.__user_name) \
                .option("password", self.__password) \
                .load()
        except AnalysisException as e:
            logging.error(e)
            return None

    def write_table(self, data: pyspark.sql.dataframe.DataFrame, table_name: str
                    , mode: str = "errorIfExists") -> bool:
        # TODO change to more secure way of connecting. e.g. using ssh keys ...
        """
        Function for writing
        Args:
            data (pyspark.sql.dataframe.DataFrame): Input dataframe
            table_name (str): The name of the table to write into it
            mode (str): Mode of writing with spark

        Returns:
            bool: True if writing to table was successful, False otherwise
        """
        try:
            data.write.format("jdbc") \
                .option("url", f"{self.__db_url}{self.__database_name}") \
                .option("dbtable", table_name) \
                .option("user", self.__user_name) \
                .option("password", self.__password) \
                .mode(mode) \
                .save()
        except AnalysisException as e:
            logging.error(e)
            return False
        return True
