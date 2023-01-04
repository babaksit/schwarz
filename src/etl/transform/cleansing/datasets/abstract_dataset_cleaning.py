from abc import abstractmethod, ABC
from typing import Any


class DataCleaningTemplate(ABC):
    """
    Abstract class for data cleaning, this class uses Template design pattern
    """

    def clean(self, data: Any) -> None:
        """
        Function for cleaning the data which uses Template design pattern
        Args:
            data (Any):

        Returns:
            None
        """
        self.check_columns(data)
        data = self.drop_invalid_rows(data)
        self.save(data)

    @abstractmethod
    def check_columns(self, data: Any):
        """
        Function for checking expected columns exist in the data
        Args:
            data (Any):

        Returns:

        """
        raise NotImplementedError

    @abstractmethod
    def drop_invalid_rows(self, data: Any):
        raise NotImplementedError

    @abstractmethod
    def save(self, data):
        raise NotImplementedError
