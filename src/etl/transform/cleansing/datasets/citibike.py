import datetime
import logging
import re
from abc import abstractmethod
from typing import Union

from pyspark.sql.functions import udf, col

from src.etl.transform.cleansing.datasets.abstract_dataset_cleaning import DataCleaningTemplate
from src.etl.transform.cleansing.exeptions.citibike.citibike_exceptions import InvalidRideIdLengthError, \
    InvalidRideIdDataError, \
    InvalidBikeTypeError, InvalidDateTimeFormatError, InvalidStationNameError, InvalidStationIdError, \
    StartTimeIsGEEndtimeError, InvalidLatLonRangeError, InvalidUserTypeError


class CitibikeDataCleaningTemplate(DataCleaningTemplate):
    expected_id_length = 16
    ride_id_col_name = 'ride_id'
    bike_type_col_name = 'rideable_type'
    valid_bike_types = ['classic_bike', 'electric_bike', 'docked_bike']
    start_time_col_name = 'started_at'
    end_time_col_name = 'ended_at'
    expected_datetime_format = '%Y-%m-%d %H:%M:%S'
    start_station_col_name = 'start_station_name'
    start_station_id_col_name = 'start_station_id'
    end_station_col_name = 'end_station_name'
    end_station_id_col_name = 'end_station_id'
    start_latitude_col_name = 'start_lat'
    start_longitude_col_name = 'start_lng'
    end_latitude_col_name = 'end_lat'
    end_longitude_col_name = 'end_lng'
    user_type_col_name = 'member_casual'
    valid_station_names = ['']
    valid_user_types = ['member', 'casual']

    @abstractmethod
    def drop_invalid_rows(self, data):
        pass

    @staticmethod
    def check_nan_col(row):
        pass

    @staticmethod
    def check_ride_id_length(ride_id, expected_id_length):
        if len(ride_id) > expected_id_length:
            raise InvalidRideIdLengthError(f"Invalid Ride id Length: {len(ride_id)}, "
                                           f"expected length is: {expected_id_length}")

    @staticmethod
    def check_ride_id_data(ride_id):
        if not ride_id.isalnum():
            raise InvalidRideIdDataError(f"Ride id should consist only of alphanumerics  : {ride_id}")

    @staticmethod
    def check_bike_type(bike_type, valid_bike_types):
        if bike_type not in valid_bike_types:
            raise InvalidBikeTypeError(f"Invalid bike type: {bike_type}, valid bike types are: {valid_bike_types}")

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
    def check_start_time_is_lt_end_time(start_time, end_time):
        if start_time >= end_time:
            raise StartTimeIsGEEndtimeError

    @staticmethod
    def check_valid_duration(start_time, end_time):
        pass

    @staticmethod
    def check_station_name(station_name):
        if not station_name:
            raise InvalidStationNameError(f"Invalid station name{station_name}")
        # if station_name not in self.valid_station_names:
        #     raise InvalidStationNameError

    @staticmethod
    def check_station_id(station_id):
        pattern = re.compile(r"^[A-Z]{2}\d{3}$")
        if not (bool(pattern.match(station_id))):
            raise InvalidStationIdError(f"Invalid Station id: {station_id}")

    @staticmethod
    def check_invalid_lat_lon(latitude, longitude):
        if not (isinstance(latitude, (int, float)) and isinstance(longitude, (int, float))):
            raise TypeError()
        if not (-90.0 <= latitude <= 90.0 and -180.0 <= longitude <= 180.0):
            raise InvalidLatLonRangeError()

    @staticmethod
    def check_user_type(user_type, valid_user_types):
        if user_type not in valid_user_types:
            raise InvalidUserTypeError()

    @staticmethod
    def filter_nan_col(row):
        pass

    @staticmethod
    def filter_invalid_ride_ids(ride_id):
        try:
            CitibikeDataCleaningTemplate.check_ride_id_length(ride_id, CitibikeDataCleaningTemplate.expected_id_length)
            CitibikeDataCleaningTemplate.check_ride_id_data(ride_id)
        except (InvalidRideIdLengthError, InvalidRideIdDataError) as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_bike_types(bike_type):
        try:
            CitibikeDataCleaningTemplate.check_bike_type(bike_type, CitibikeDataCleaningTemplate.valid_bike_types)
        except InvalidBikeTypeError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_duration_trips(start_time, end_time):
        try:
            CitibikeDataCleaningTemplate.check_datetime_format(start_time,
                                                               CitibikeDataCleaningTemplate.expected_datetime_format)
            CitibikeDataCleaningTemplate.check_datetime_format(end_time,
                                                               CitibikeDataCleaningTemplate.expected_datetime_format)
            CitibikeDataCleaningTemplate.check_start_time_is_lt_end_time(start_time, end_time)
            CitibikeDataCleaningTemplate.check_valid_duration(start_time, end_time)
        except (InvalidDateTimeFormatError, StartTimeIsGEEndtimeError) as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_start_station_name(start_station_name):
        try:
            CitibikeDataCleaningTemplate.check_station_name(start_station_name)
        except InvalidDateTimeFormatError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_end_station_name(end_station_name):
        try:
            CitibikeDataCleaningTemplate.check_station_name(end_station_name)
        except InvalidDateTimeFormatError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_start_station_id(start_station_id):
        try:
            CitibikeDataCleaningTemplate.check_station_id(start_station_id)
        except InvalidStationIdError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_end_station_id(end_station_id):
        try:
            CitibikeDataCleaningTemplate.check_station_id(end_station_id)
        except InvalidStationIdError as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_lat_lon(start_latitude: float, end_latitude: float,
                               start_longitude: float, end_longitude: float):
        try:
            CitibikeDataCleaningTemplate.check_invalid_lat_lon(start_latitude, start_longitude)
            CitibikeDataCleaningTemplate.check_invalid_lat_lon(end_latitude, end_longitude)
        except (TypeError, InvalidLatLonRangeError) as e:
            logging.error(e)
            return False
        return True

    @staticmethod
    def filter_invalid_user_type(user_type):
        try:
            CitibikeDataCleaningTemplate.check_user_type(user_type, CitibikeDataCleaningTemplate.valid_user_types)
        except InvalidUserTypeError as e:
            logging.error(e)
            return False
        return True


class CitibikeDataCleaningWithSpark(CitibikeDataCleaningTemplate):

    def __init__(self, spark_handler, result_table_name: str):
        super().__init__()
        self.spark_handler = spark_handler
        self.result_table_name = result_table_name

    def drop_invalid_rows(self, data):
        data = data.na.drop()
        data = data.filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_ride_ids, BooleanType())
                           (col(CitibikeDataCleaningTemplate.ride_id_col_name))) \
            .filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_bike_types, BooleanType())
                    (col(CitibikeDataCleaningTemplate.bike_type_col_name))) \
            .filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_duration_trips, BooleanType())
                    (col(CitibikeDataCleaningTemplate.start_time_col_name),
                     col(CitibikeDataCleaningTemplate.end_time_col_name))) \
            .filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_start_station_name, BooleanType())
                    (col(CitibikeDataCleaningTemplate.start_station_col_name))) \
            .filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_end_station_name, BooleanType())
                    (col(CitibikeDataCleaningTemplate.end_station_col_name))) \
            .filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_start_station_id, BooleanType())
                    (col(CitibikeDataCleaningTemplate.start_station_id_col_name))) \
            .filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_end_station_id, BooleanType())
                    (col(CitibikeDataCleaningTemplate.end_station_id_col_name))) \
            .filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_lat_lon, BooleanType())
                    (col(CitibikeDataCleaningTemplate.start_latitude_col_name),
                     col(CitibikeDataCleaningTemplate.start_longitude_col_name),
                     col(CitibikeDataCleaningTemplate.end_latitude_col_name),
                     col(CitibikeDataCleaningTemplate.end_longitude_col_name)
                     )) \
            .filter(udf(CitibikeDataCleaningWithSpark.filter_invalid_user_type, BooleanType())
                    (col(CitibikeDataCleaningTemplate.user_type_col_name)))

        return data

    def save(self, data):
        self.spark_handler.write_table_error_if_exists(data, self.result_table_name)

    def check_columns(self, data):
        pass
