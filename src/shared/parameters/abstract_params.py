from abc import ABC, abstractmethod
from typing import Any


class AbstractParams(ABC):
    """Abstract class for representing parameters"""
    @abstractmethod
    def read(self, params: Any) -> None:
        """
        Function for reading parameters
        Args:
            params (Any): Input params

        Raises:
            NotImplementedError

        """
        raise NotImplementedError

    @abstractmethod
    def check_keys_vars(self,  params: dict):
        """
        Function for checking if all the class variables exist in the parameters
        Args:
            params (dict): Input params

        Raises:
            NotImplementedError
        """
        raise NotImplementedError
