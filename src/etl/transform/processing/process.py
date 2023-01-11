import logging
from abc import abstractmethod, ABC
from typing import List
import pyspark
from pyspark.sql.utils import AnalysisException
from src.utils.spark import SparkHandler


class Processor(ABC):
    """Abstract processor class for processing the cleaned data"""

    def execute(self) -> bool:
        """
        Template function for ordering the execution of the functions
        Returns:
            bool: True if process was successful, False otherwise
        """
        success = False
        if self.read():
            data = self.process()
            success = self.save(data)
        return success

    @abstractmethod
    def read(self):
        """
        Abstract method to read data
        Raises:
            NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def process(self):
        """
        Abstract method to process data
        Raises:
            NotImplementedError
        """
        raise NotImplementedError

    @abstractmethod
    def save(self, data):
        """
        Abstract method to save the result data
        Raises:
            NotImplementedError
        """
        raise NotImplementedError


class SparkProcess(Processor):
    """Class for processing spark data"""

    def __init__(self, spark_handler: SparkHandler, tables: List[str],
                 query_path: str, result_table_name: str):
        """
        Init SparkProcess class
        Args:
            spark_handler (SparkHandler): An instance of SparkHandler
            tables (List[str]): List of table names
            query_path (str): The path to the sql query
            result_table_name (str): The result table name
        """
        self.__result_table_name = result_table_name
        self._spark_handler = spark_handler
        self._tables = tables
        self._query_path = query_path

    def read(self) -> bool:
        """
        Function for reading data with spark
        Returns:
            bool: True if reading was successful, False otherwise
        """
        try:
            for table in self._tables:
                self._spark_handler.read_table(table).createOrReplaceTempView(table)
        except AnalysisException as e:
            logging.error(e)
            return False
        return True

    @abstractmethod
    def process(self) -> None:
        """
        Abstract method to process data
        Raises:
            NotImplementedError
        """
        raise NotImplementedError

    def save(self, data: pyspark.sql.dataframe.DataFrame) -> bool:
        """
        Function for saving the results into table
        Args:
            data (pyspark.sql.dataframe.DataFrame): The processed data

        Returns:
            bool: True if writing to table was successful, False otherwise
        """
        return self._spark_handler.write_table(data, self.__result_table_name)


class ProcessCitiBikeWeatherDataWithSpark(SparkProcess):

    def __init__(self, spark_handler, tables, query_path, result_table_name):
        """
        Init ProcessCitiBikeWeatherDataWithSpark class
        Args:
            spark_handler (SparkHandler): An instance of SparkHandler
            tables (List[str]): List of table names
            query_path (str): The path to the sql query
            result_table_name (str): The result table name
        """
        super().__init__(spark_handler, tables, query_path, result_table_name)

    def process(self) -> pyspark.sql.dataframe.DataFrame:
        """
        Function for processing spark data using the specified sql query
        Returns:
            pyspark.sql.dataframe.DataFrame: The processed data
        """
        result = None
        try:
            result = self._spark_handler.spark.sql(open(self._query_path).read())
        except AnalysisException as e:
            logging.error(e)
        return result
