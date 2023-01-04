import datetime
import logging
from abc import abstractmethod
from typing import Union

from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType

from src.etl.transform.cleansing.datasets.abstract_dataset_cleaning import DataCleaningTemplate
from src.etl.transform.cleansing.exeptions.citibike.citibike_exceptions import InvalidDateTimeFormatError


class WeatherDataCleaningTemplate(DataCleaningTemplate):
    expected_date_time_format = '%Y-%m-%dT%H:%M:%S'
    city_names = ['nyc']
    city_col_name = 'name'
    temperature_col_name = 'temp'
    date_time_col_name = 'datetime'

    def save(self, data):
        pass

    @abstractmethod
    def drop_invalid_rows(self, data):
        pass

    @staticmethod
    def check_nan_col(row):
        pass

    @staticmethod
    def check_city(city):
        if city not in WeatherDataCleaningTemplate.city_names:
            raise ValueError(f"city: {city} is not in expected city names")

    @staticmethod
    def check_datetime_format(date_time: Union[str, datetime.datetime], expected_datetime_format: str):
        try:
            if isinstance(date_time, str):
                datetime.datetime.strptime(date_time, expected_datetime_format)
            elif isinstance(date_time, datetime.datetime):
                datetime.datetime.strftime(date_time, expected_datetime_format)
        except ValueError:
            raise InvalidDateTimeFormatError(f"Invalid timestamp format: {date_time}, "
                                             f"expected format: {expected_datetime_format}")

    @staticmethod
    def check_temp(temp):
        if -459.67 <= temp <= 212:
            return
        raise ValueError(f"temperature {temp} is not within expected range")

    @staticmethod
    def filter_invalid_city(city):
        try:
            WeatherDataCleaningTemplate.check_city(city)
        except ValueError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_datetime(date_time):
        try:
            WeatherDataCleaningTemplate.check_datetime_format(date_time,
                                                              WeatherDataCleaningTemplate.expected_date_time_format)
        except InvalidDateTimeFormatError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_temp(temp):
        try:
            WeatherDataCleaningTemplate.check_temp(temp)
        except ValueError as e:
            logging.error(e)
            return False
        return True


class WeatherDataCleaningWithSpark(WeatherDataCleaningTemplate):
    def __init__(self, spark_handler, result_table_name: str):
        super().__init__()
        self.spark_handler = spark_handler
        self.result_table_name = result_table_name

    def save(self, data):
        self.spark_handler.write_table_error_if_exists(data, self.result_table_name)

    def check_columns(self, data):
        pass

    def drop_invalid_rows(self, data):
        data = data.na.drop()
        data = data.filter(udf(WeatherDataCleaningWithSpark.filter_invalid_temp, BooleanType())
                           (col(WeatherDataCleaningWithSpark.temperature_col_name))) \
            .filter(udf(WeatherDataCleaningWithSpark.filter_invalid_city, BooleanType())
                    (col(WeatherDataCleaningWithSpark.city_col_name))) \
            .filter(udf(WeatherDataCleaningWithSpark.filter_invalid_datetime, BooleanType())
                    (col(WeatherDataCleaningWithSpark.date_time_col_name)))
        return data