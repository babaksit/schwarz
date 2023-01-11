from typing import Any

from src.shared.parameters.abstract_params import AbstractParams


class JsonParams(AbstractParams):
    """Class for representing Json type parameters"""
    @classmethod
    def read(cls, params: Any) -> None:
        """
        Function for reading parameters
        Args:
            params (Any): Input params
        Raises:
            ValueError if some keys are missing in given parameters
        Returns:
            None
        """
        if not cls.check_keys_vars(params):
            raise ValueError('Missing required keys')
        for key, value in params.items():
            setattr(cls, key, value)

    @classmethod
    def check_keys_vars(cls, params: dict) -> bool:
        """
        Function for checking if all the class variables exist in the parameters
        Args:
            params (dict): Input params

        Raises:
            NotImplementedError
        Returns:
            bool: True if all the class variables exist in the parameters, Otherwise False
        """
        cls_vars = dir(cls)
        return all(key in cls_vars for key in params)
