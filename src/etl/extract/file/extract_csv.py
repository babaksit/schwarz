from abc import ABC, abstractmethod
from src.utilities.spark import SparkHandler


class DBNotExistsError(Exception):
    """Exception class when Database does not exist"""
    pass


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
        self._spark_handler = spark_handler
        self._file_path = file_path
        self._result_table_name = result_table_name

    def extract(self) -> None:
        """
        Function for extracting data from a csv file into Postgres

        Returns:
            None
        """
        # Read the CSV file into a DataFrame
        df = self._spark_handler.spark.read.csv(self._file_path, header=True, inferSchema=True)
        # Write the DataFrame to the PostgresSQL database
        self._spark_handler.write_table_error_if_exists(df, self._result_table_name)
