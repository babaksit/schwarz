import datetime
import logging
from abc import abstractmethod
from typing import Any

import pyspark
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType

from src.etl.transform.cleansing.datasets.abstract_dataset_cleaning import DataCleaningTemplate
from src.etl.transform.cleansing.exeptions.shared_exceptions import ColumnMissingError, ColumnTypeError
from src.etl.transform.cleansing.exeptions.weather_exceptions import InvalidDateTimeFormatError, \
    InvalidTemperatureRangeError, InvalidCityNameError
from src.shared.parameters.weather_params import WeatherParams
from src.utils.spark import SparkHandler
from src.utils.typedefs import City, Temperature, DateTimeFormat


class WeatherDataCleaningTemplate(DataCleaningTemplate):
    @abstractmethod
    def save(self, data: Any):
        """
        Function for saving the filtered data into a table
        Args:
            data (pyspark.sql.dataframe.DataFrame): Filtered data

        Returns:
            None:
        Raises:
            NotImplementedError: The abstract function should not be called
        """
        raise NotImplementedError

    @abstractmethod
    def drop_invalid_rows(self, data: Any):
        """
        Abstract method to drop invalid rows
        Args:
            data (Any): Input data
        Returns:
            None:
        Raises:
            NotImplementedError: The abstract function should not be called
        """
        raise NotImplementedError

    @staticmethod
    def check_city(city: City) -> None:
        """
        Function to check if city name is in valid list of city names
        Args:
            city (City): The name of the city
        Returns:
            None:
        Raises:
            InvalidCityNameError: If the city name is invalid
        """
        if city not in WeatherParams.city_names:
            raise InvalidCityNameError(f"city: {city} is not in expected city names")

    @staticmethod
    def check_datetime_format(date_time: datetime.datetime, expected_datetime_format: DateTimeFormat) -> None:
        """
        Function for checking the date_time format
        Args:
            date_time (datetime.datetime): Input date_time to be chekced
            expected_datetime_format (DateTimeFormat): Expected date_time format
        Returns:
            None:
        Raises:
            TypeError: if date_time type is not an instance of datetime.datetime
            InvalidDateTimeFormatError: if date_time is not in expcted format
        """
        try:
            if isinstance(date_time, datetime.datetime):
                datetime.datetime.strftime(date_time, expected_datetime_format)
            else:
                raise TypeError(f"date_time should be of type datetime.datetime")
        except ValueError:
            raise InvalidDateTimeFormatError(f"Invalid timestamp format: {date_time}, "
                                             f"expected format: {expected_datetime_format}")

    @staticmethod
    def check_temp(temp: Temperature) -> None:
        """
        Function for checking if the temp is in correct range
        Args:
            temp (Temperature): Temperature value in Fahrenheit

        Returns:
            None:
        Raises:
            InvalidTemperatureRangeError: If the temperature is not in valid range
        """
        if WeatherParams.min_temp <= temp <= WeatherParams.max_temp:
            return
        raise InvalidTemperatureRangeError(f"temperature {temp} is not within expected range"
                                           f"{WeatherParams.min_temp} and {WeatherParams.max_temp}")

    @staticmethod
    def filter_invalid_city(city: City) -> bool:
        """
        Function for checking if city name is not in valid city names
        Args:
            city (City): City name

        Returns:
            True if the city should be kept otherwise False

        """
        try:
            WeatherDataCleaningTemplate.check_city(city)
        except InvalidCityNameError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_datetime(date_time: datetime.datetime) -> bool:
        """
        Function for checking if date_time is valid
        Args:
            date_time (datetime.datetime):

        Returns:
            True if the date_time should be kept otherwise False
        """
        try:
            WeatherDataCleaningTemplate.check_datetime_format(date_time,
                                                              WeatherParams.expected_date_time_format)
        except InvalidDateTimeFormatError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_temp(temp: Temperature) -> bool:
        """
        Function for checking if temp is valid
        Args:
            temp (Temperature): temperature value in Fahrenheit

        Returns:
            True if the temp should be kept otherwise False
        """
        try:
            WeatherDataCleaningTemplate.check_temp(temp)
        except InvalidTemperatureRangeError as e:
            logging.error(e)
            return False
        return True


class WeatherDataCleaningWithSpark(WeatherDataCleaningTemplate):
    def __init__(self, spark_handler: SparkHandler, result_table_name: str):
        """
        Initialize WeatherDataCleaningWithSpark
        Args:
            spark_handler (SparkHandler): An instance of the SparkHandler class
            result_table_name (str): The name of result table that should be saved
        """
        super().__init__()
        self.spark_handler = spark_handler
        self.result_table_name = result_table_name
        # TODO move type map to seperate class
        self.type_map = {
            WeatherParams.temperature_col_name: pyspark.sql.types.DoubleType,
            WeatherParams.city_col_name: pyspark.sql.types.StringType,
            WeatherParams.date_time_col_name: pyspark.sql.types.TimestampType,
        }

    def check_columns(self, data: pyspark.sql.dataframe.DataFrame) -> None:
        """
        Function for checking if all the expected columns exist in the data
        Args:
            data (pyspark.sql.dataframe.DataFrame): Input data
        Raises:
            ColumnMissingError: If expected columns are not match
        Returns:
            None
        """
        if set(data.columns) != set(WeatherParams.columns):
            raise ColumnMissingError(
                f"Expected columns are not match: {set(WeatherParams.columns) - set(data.columns)}")

    def check_types(self, data: pyspark.sql.dataframe.DataFrame) -> None:
        """
        Function for checking types of columns
        Args:
            data (pyspark.sql.dataframe.DataFrame): Input data

        Raises:
            ColumnTypeError: If a columns has incorrect type
        Returns:
            None:
        """
        for col_name, dtype in self.type_map.items():
            if not data.schema[col_name].dataType != dtype:
                raise ColumnTypeError(f" Column{col_name} has incorrect type: {type(data.schema[col_name].dataType)}"
                                      f"Expected type is:{dtype}")

    def save(self, data: pyspark.sql.dataframe.DataFrame) -> bool:
        """
        Function for saving the filtered data into a table
        Args:
            data (pyspark.sql.dataframe.DataFrame): Filtered data

        Returns:
            bool: True if writing to table was successful, False otherwise
        """
        return self.spark_handler.write_table(data, self.result_table_name)

    def drop_invalid_rows(self, data) -> pyspark.sql.dataframe.DataFrame:
        """
        Function for dropping invalid rows
        Args:
            data (pyspark.sql.dataframe.DataFrame): Input data

        Returns:
            pyspark.sql.dataframe.DataFrame: Filtered data
        """
        data = data.na.drop()
        data = data.filter(udf(self.filter_invalid_temp, BooleanType())
                           (col(WeatherParams.temperature_col_name))) \
            .filter(udf(self.filter_invalid_city, BooleanType())
                    (col(WeatherParams.city_col_name))) \
            .filter(udf(self.filter_invalid_datetime, BooleanType())
                    (col(WeatherParams.date_time_col_name)))
        return data
