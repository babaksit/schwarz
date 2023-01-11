import logging
from abc import abstractmethod, ABC
from typing import Any
from src.etl.transform.cleansing.exeptions.shared_exceptions import ColumnMissingError, ColumnTypeError


class DataCleaningTemplate(ABC):
    """
    Abstract class for data cleaning, this class uses Template design pattern
    """

    def clean(self, data: Any) -> bool:
        """
        Function for cleaning the data which uses Template design pattern
        Args:
            data (Any): Input data

        Returns:
            bool: True if cleaning operation was done successfully, False otherwise
        """
        try:
            self.check_columns(data)
            self.check_types(data)
        except (ColumnMissingError, ColumnTypeError) as e:
            logging.error(e)
            return False
        data = self.drop_invalid_rows(data)
        success = self.save(data)

        return success

    @abstractmethod
    def check_columns(self, data: Any) -> None:
        """
        Function for checking expected columns exist in the data
        Args:
            data (Any): Input data

        Returns:
            None:
        """
        raise NotImplementedError

    @abstractmethod
    def check_types(self, data: Any) -> None:
        """
        Function for checking types of columns in data
        Args:
            data (Any): Input data

        Returns:
            None:
        """
        raise NotImplementedError

    @abstractmethod
    def drop_invalid_rows(self, data: Any) -> None:
        """
        Abstract function for dropping invalid rows
        Args:
            data (Any): Input data

        Returns:
            None:
        """
        raise NotImplementedError

    @abstractmethod
    def save(self, data) -> None:
        """
        Abstract function for saving the results
        Args:
            data (Any): Input data

        Returns:
            None:
        """
        raise NotImplementedError
