import datetime
import logging
import re
from abc import abstractmethod
from typing import Any

import pyspark
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType

import src.utils.spark
from src.etl.transform.cleansing.datasets.abstract_dataset_cleaning import DataCleaningTemplate
from src.etl.transform.cleansing.exeptions.citibike_exceptions import *
from src.etl.transform.cleansing.exeptions.shared_exceptions import ColumnMissingError, ColumnTypeError
from src.shared.parameters.citibike_params import CitibikeParams
from src.utils.typedefs import *


class CitibikeDataCleaningTemplate(DataCleaningTemplate):
    """ Citibike data cleaning template class"""

    @abstractmethod
    def drop_invalid_rows(self, data: Any) -> None:
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

    @abstractmethod
    def check_columns(self, data: pyspark.sql.dataframe.DataFrame) -> None:
        """
        Function for checking if all the expected columns exist in the data
        Args:
            data (pyspark.sql.dataframe.DataFrame): Input data

        Returns:
            None:
        Raises:
            NotImplementedError: The abstract function should not be called
        """
        raise NotImplementedError

    @staticmethod
    def check_ride_id_length(ride_id: RideId, expected_id_length: int) -> None:
        """
        Static method for checking if the length of ride id is correct
        Args:
            ride_id (str): Ride id
            expected_id_length (int): Expected length of ride id

        Returns:
            None:
        Raises:
            InvalidRideIdLengthError: If length of the ride_id is incorrect

        """
        if len(ride_id) != expected_id_length:
            raise InvalidRideIdLengthError(f"Invalid Ride id Length: {len(ride_id)}, "
                                           f"expected length is: {expected_id_length}")

    @staticmethod
    def check_ride_id_data(ride_id: RideId) -> None:
        """
        Static method for checking if the ride id data is correct
        Args:
            ride_id (RideId): Ride id

        Returns:
            None:
        Raises:
            InvalidRideIdDataError: If the data of the ride is incorrect

        """
        if not ride_id.isalnum():
            raise InvalidRideIdDataError(f"Ride id should consist only of alphanumerics  : {ride_id}")

    @staticmethod
    def check_bike_type(bike_type: Bike, valid_bike_types: BikeList) -> None:
        """
        Static method to check if the bike type is in valid list of bikes
        Args:
            bike_type (Bike): Type of bike
            valid_bike_types (BikeList): List of valid bike types

        Returns:
            None:
        Raises:
            InvalidBikeTypeError: If the type of bike is not valid

        """
        if bike_type not in valid_bike_types:
            raise InvalidBikeTypeError(f"Invalid bike type: {bike_type}, valid bike types are: {valid_bike_types}")

    @staticmethod
    def check_datetime_format(date_time: datetime.datetime, expected_datetime_format: DateTimeFormat) -> None:
        """
        Static method to check if the datetime format is correct
        Args:
            date_time (datetime.datetime):
            expected_datetime_format (DateTimeFormat):

        Returns:
            None:
        Raises:
            InvalidDateTimeFormatError: If Datetime format is invalid
        """
        try:
            if isinstance(date_time, datetime.datetime):
                datetime.datetime.strftime(date_time, expected_datetime_format)
        except ValueError:
            raise InvalidDateTimeFormatError(f"Invalid timestamp format: {date_time}, "
                                             f"expected format: {expected_datetime_format}")

    @staticmethod
    def check_start_time_is_lt_end_time(start_time: datetime.datetime, end_time: datetime.datetime) -> None:
        """
        Static method to check if the start time is less than the end time
        Args:
            start_time (datetime.datetime): start time
            end_time (datetime.datetime): end time

        Returns:
            None:
        Raises:
            StartTimeIsGEEndtimeError: if the start time is greater than the end time
        """
        if start_time >= end_time:
            raise StartTimeIsGEEndtimeError(f"start time {start_time} is greater than the end time{end_time}")

    @staticmethod
    def check_valid_duration(start_time: datetime.datetime, end_time: datetime.datetime) -> None:
        """
        Static method to check if the duration of the trip is valid

        Args:
            start_time (datetime.datetime):
            end_time (datetime.datetime):

        Returns:
            None:
        Raises:
            NotImplementedError: It is not implemented yet
        """
        raise NotImplementedError

    @staticmethod
    def check_station_name(station_name: StationName) -> None:
        """
        Static method to check if the station name is valid
        Args:
            station_name (StationName): The name of the station

        Returns:
            None:
        Raises:
            InvalidStationNameError: Ff the station name is invalid
        """
        if not station_name:
            raise InvalidStationNameError(f"Invalid station name{station_name}")
        # TODO add valid station names
        # if station_name not in self.valid_station_names:
        #     raise InvalidStationNameError

    @staticmethod
    def check_station_id(station_id: StationId) -> None:
        """
        Static method to check if the station id is valid by checking the expected pattern of the id
        Args:
            station_id (StationId): The id of the station

        Returns:
            None:
        Raises:
            InvalidStationIdError: If the pattern of station id is invalid
        """
        pattern = re.compile(r"^[A-Z]{2}\d{3}$")
        if not (bool(pattern.match(station_id))):
            raise InvalidStationIdError(f"Invalid Station id: {station_id} the pattern should match ^[A-Z]{2}\d{3}$")

    @staticmethod
    def check_invalid_lat(latitude: Latitude) -> None:
        """
        Static method to check if the Latitude is valid by checking the expected range of Latitude
        Args:
            latitude (Latitude): latitude value

        Returns:
            None:
        Raises:
            InvalidLatRangeError: If the latitude is not in valid range
        """
        if not (CitibikeParams.min_latitude <= latitude <= CitibikeParams.max_latitude):
            raise InvalidLatRangeError(f"Invalid latitude: {latitude}, "
                                       f"should be between {CitibikeParams.min_latitude} "
                                       f"and {CitibikeParams.max_latitude}")

    @staticmethod
    def check_invalid_lon(longitude: Longitude) -> None:
        """
        Static method to check if the longitude is valid by checking the expected range of Longitude
        Args:
            longitude (Longitude): longitude value

        Returns:
            None:
        Raises:
            InvalidLonRangeError: If the Longitude is not in valid range
        """
        if not (CitibikeParams.min_longitude <= longitude <= CitibikeParams.max_longitude):
            raise InvalidLonRangeError(f"Invalid longitude range: {longitude} "
                                       f"should be between {CitibikeParams.min_longitude} and"
                                       f" {CitibikeParams.max_longitude}")

    @classmethod
    def check_invalid_lat_lon(cls, latitude: Latitude, longitude: Longitude) -> None:
        """
        Static method to check if the latitude and longitude is valid by checking the expected range of them
        Args:
            latitude (Latitude): latitude value
            longitude (Longitude): longitude value

        Returns:
            None:
        """
        cls.check_invalid_lat(latitude)
        cls.check_invalid_lon(longitude)

    @staticmethod
    def check_user_type(user_type: UserType, valid_user_types: UserList) -> None:
        """
        Static method to check if the user type is in valid list of user types
        Args:
            user_type (UserType): The type of the user
            valid_user_types (UserList): List of valid user types

        Returns:
            None:
        Raises:
            InvalidUserTypeError: If user type is invalid
        """
        if user_type not in valid_user_types:
            raise InvalidUserTypeError(f"invalid user type {user_type}, should be one of {valid_user_types}")

    @classmethod
    def filter_invalid_ride_ids(cls, ride_id: RideId) -> bool:
        """
        Static method to filter the ride_id
        Args:
            ride_id (RideId): Ride id value

        Returns:
            True if the ride_id should be kept otherwise False
        """
        try:
            cls.check_ride_id_length(ride_id, CitibikeParams.expected_ride_id_length)
            cls.check_ride_id_data(ride_id)
        except (InvalidRideIdLengthError, InvalidRideIdDataError) as e:
            logging.error(e)
            return False
        return True

    @classmethod
    def filter_invalid_bike_types(cls, bike_type: Bike) -> bool:
        """
        Static method to filter the bike_type
        Args:
            bike_type (Bike): The type of the bike

        Returns:
            True if the bike_type should be kept otherwise False
        """
        try:
            cls.check_bike_type(bike_type, CitibikeParams.valid_bike_types)
        except InvalidBikeTypeError as e:
            logging.error(e)
            return False
        return True

    @classmethod
    def filter_invalid_duration_trips(cls, start_time: datetime.datetime,
                                      end_time: datetime.datetime) -> bool:
        """
        Static method to filter the start_time and end_time
        Args:
            start_time (datetime.datetime): Start time
            end_time (datetime.datetime): End time

        Returns:
            True if the start_time and end_time should be kept otherwise False

        """
        try:
            cls.check_datetime_format(start_time, CitibikeParams.expected_datetime_format)
            cls.check_datetime_format(end_time, CitibikeParams.expected_datetime_format)
            cls.check_start_time_is_lt_end_time(start_time, end_time)
        except (InvalidDateTimeFormatError, StartTimeIsGEEndtimeError) as e:
            logging.error(e)
            return False
        return True

    @classmethod
    def filter_invalid_start_station_name(cls, start_station_name: StationName) -> bool:
        """
        Static method to filter the start_station_name
        Args:
            start_station_name (StationName): The name of the start station

        Returns:
            True if the start_station_name should be kept otherwise False

        """
        try:
            cls.check_station_name(start_station_name)
        except InvalidDateTimeFormatError as e:
            logging.error(e)
            return False
        return True

    @classmethod
    def filter_invalid_end_station_name(cls, end_station_name: StationName) -> bool:
        """
        Static method to filter the end_station_name
        Args:
            end_station_name (StationName): The name of the end station

        Returns:
            True if the end_station_name should be kept otherwise False

        """
        try:
            cls.check_station_name(end_station_name)
        except InvalidDateTimeFormatError as e:
            logging.error(e)
            return False
        return True

    @classmethod
    def filter_invalid_start_station_id(cls, start_station_id: StationId) -> bool:
        """
        Static method to filter the start_station_id
        Args:
            start_station_id (StationId): The id of the start station

        Returns:
            True if the start_station_id should be kept otherwise False

        """
        try:
            cls.check_station_id(start_station_id)
        except InvalidStationIdError as e:
            logging.error(e)
            return False
        return True

    @classmethod
    def filter_invalid_end_station_id(cls, end_station_id: StationId) -> bool:
        """
        Static method to filter the end_station_id
        Args:
            end_station_id (StationId): The id of the end station

        Returns:
            True if the end_station_id should be kept otherwise False

        """
        try:
            cls.check_station_id(end_station_id)
        except InvalidStationIdError as e:
            logging.error(e)
            return False
        return True

    @classmethod
    def filter_invalid_lat_lon(cls, start_latitude: Latitude, end_latitude: Latitude,
                               start_longitude: Longitude, end_longitude: Longitude) -> bool:
        """
        Static method to filter the start_latitude, end_latitude, start_longitude, end_longitude
        Args:
            start_latitude (Latitude): Start latitude value
            end_latitude (Latitude): End latitude value
            start_longitude (Longitude): Start longitude value
            end_longitude (Longitude): End longitude value

        Returns:
            True if the start and end latitudes and longitudes should be kept otherwise False

        """
        try:
            cls.check_invalid_lat_lon(start_latitude, start_longitude)
            cls.check_invalid_lat_lon(end_latitude, end_longitude)
        except (TypeError, InvalidLatRangeError, InvalidLonRangeError) as e:
            logging.error(e)
            return False
        return True

    @classmethod
    def filter_invalid_user_type(cls, user_type: UserType) -> bool:
        """
        Static method to filter the user_type
        Args:
            user_type (UserType): The type of the user

        Returns:
            True if the user_type should be kept otherwise False

        """
        try:
            cls.check_user_type(user_type, CitibikeParams.valid_user_types)
        except InvalidUserTypeError as e:
            logging.error(e)
            return False
        return True


class CitibikeDataCleaningWithSpark(CitibikeDataCleaningTemplate):

    def __init__(self, spark_handler: src.utils.spark.SparkHandler, result_table_name: str):
        """
        Init CitibikeDataCleaningWithSpark class
        Args:
            spark_handler (SparkHandler): An instance of the SparkHandler class
            result_table_name (str): The name of result table that should be saved
        """
        super().__init__()
        self.spark_handler = spark_handler
        self.result_table_name = result_table_name
        # TODO move type map to separate class
        self.type_map = {
            CitibikeParams.ride_id_col_name: pyspark.sql.types.StringType,
            CitibikeParams.bike_type_col_name: pyspark.sql.types.StringType,
            CitibikeParams.start_time_col_name: pyspark.sql.types.TimestampType,
            CitibikeParams.end_time_col_name: pyspark.sql.types.TimestampType,
            CitibikeParams.start_station_col_name: pyspark.sql.types.StringType,
            CitibikeParams.end_station_col_name: pyspark.sql.types.StringType,
            CitibikeParams.start_station_id_col_name: pyspark.sql.types.StringType,
            CitibikeParams.end_station_id_col_name: pyspark.sql.types.StringType,
            CitibikeParams.start_latitude_col_name: pyspark.sql.types.DoubleType,
            CitibikeParams.start_longitude_col_name: pyspark.sql.types.DoubleType,
            CitibikeParams.end_latitude_col_name: pyspark.sql.types.DoubleType,
            CitibikeParams.end_longitude_col_name: pyspark.sql.types.DoubleType,
            CitibikeParams.user_type_col_name: pyspark.sql.types.StringType
        }

    def check_types(self, data: pyspark.sql.dataframe.DataFrame):
        """
        Function for checking types of columns
        Args:
            data (pyspark.sql.dataframe.DataFrame): Input data

        Raises:
            ColumnTypeError: If a columns has incorrect type
        """
        for col_name, dtype in self.type_map.items():
            if not data.schema[col_name].dataType != dtype:
                raise ColumnTypeError(f" Column{col_name} has incorrect type: {type(data.schema[col_name].dataType)}"
                                      f"Expected type is:{dtype}")

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
        if set(CitibikeParams.columns) != set(data.columns):
            raise ColumnMissingError(
                f"Expected columns are not match: {set(CitibikeParams.columns) - set(data.columns)}")

    def drop_invalid_rows(self, data: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        """
        Function for dropping invalid rows
        Args:
            data (pyspark.sql.dataframe.DataFrame):

        Returns:
            pyspark.sql.dataframe.DataFrame: The dataframe after filtering
        """
        data = data.na.drop()
        #TODO should it be replaced by map here?
        data = data.filter(udf(self.filter_invalid_ride_ids, BooleanType())
                           (col(CitibikeParams.ride_id_col_name))) \
            .filter(udf(self.filter_invalid_bike_types, BooleanType())
                    (col(CitibikeParams.bike_type_col_name))) \
            .filter(udf(self.filter_invalid_duration_trips, BooleanType())
                    (col(CitibikeParams.start_time_col_name),
                     col(CitibikeParams.end_time_col_name))) \
            .filter(udf(self.filter_invalid_start_station_name, BooleanType())
                    (col(CitibikeParams.start_station_col_name))) \
            .filter(udf(self.filter_invalid_end_station_name, BooleanType())
                    (col(CitibikeParams.end_station_col_name))) \
            .filter(udf(self.filter_invalid_start_station_id, BooleanType())
                    (col(CitibikeParams.start_station_id_col_name))) \
            .filter(udf(self.filter_invalid_end_station_id, BooleanType())
                    (col(CitibikeParams.end_station_id_col_name))) \
            .filter(udf(self.filter_invalid_lat_lon, BooleanType())
                    (col(CitibikeParams.start_latitude_col_name),
                     col(CitibikeParams.start_longitude_col_name),
                     col(CitibikeParams.end_latitude_col_name),
                     col(CitibikeParams.end_longitude_col_name)
                     )) \
            .filter(udf(self.filter_invalid_user_type, BooleanType())
                    (col(CitibikeParams.user_type_col_name)))

        return data

    def save(self, data: pyspark.sql.dataframe.DataFrame) -> bool:
        """
        Function for saving the filtered data into a table
        Args:
            data (pyspark.sql.dataframe.DataFrame): Filtered data

        Returns:
            bool: True if saving to table was successful, False otherwise
        """
        return self.spark_handler.write_table(data, self.result_table_name)
