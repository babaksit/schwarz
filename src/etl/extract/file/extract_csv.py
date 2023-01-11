import logging
from abc import ABC, abstractmethod
from pyspark.sql.utils import AnalysisException
from src.utils.spark import SparkHandler


class DataExtract(ABC):
    """
    Abstract class for extracting data
    """

    @abstractmethod
    def extract(self) -> None:
        """
        Abstract Function for extracting data

        Returns:
            None
        """
        raise NotImplementedError


class CSVDataExtract(DataExtract):
    """
    Abstract class for extracting data from CSV file
    """

    @abstractmethod
    def extract(self) -> None:
        """
        Abstract Function for extracting data from a csv file

        Returns:
            None
        """
        raise NotImplementedError


class PostgresCSVDataExtract(CSVDataExtract):
    """
    Class for extracting data from a csv file into Postgres
    """

    def __init__(self, spark_handler: SparkHandler, file_path: str, result_table_name: str):
        """
        Initializing the PostgresCSVDataExtract
        Args:
            spark_handler (src.utilities.spark.SparkHandler): SprakHandler instance
            file_path (str): File path where csv file resides
            result_table_name (str): The name of the table for saving the result
        """
        self.__spark_handler = spark_handler
        self.__file_path = file_path
        self.__result_table_name = result_table_name

    def extract(self) -> bool:
        """
        Function for extracting data from a csv file into Postgres

        Returns:
            bool: If extracting was successful, False otherwise
        """
        try:
            # Read the CSV file into a DataFrame
            df = self.__spark_handler.spark.read.csv(self.__file_path, header=True, inferSchema=True)
        except AnalysisException as e:
            logging.error(e)
            return False

        # Write the DataFrame to the PostgresSQL database
        success = self.__spark_handler.write_table(df, self.__result_table_name)
        return success
