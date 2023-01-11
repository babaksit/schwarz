import datetime

from src.shared.parameters.json_params import JsonParams
from src.utils.typedefs import *


class CitibikeParams(JsonParams):
    columns = ['ride_id', 'rideable_type',
               'started_at', 'ended_at',
               'start_station_name', 'start_station_id',
               'end_station_name', 'end_station_id',
               'start_lat', 'start_lng',
               'end_lat', 'end_lng',
               'member_casual']

    expected_ride_id_length = 16
    min_latitude = -90.0
    max_latitude = 90.0
    min_longitude = -180.0
    max_longitude = 180.0
    valid_bike_types = ['classic_bike', 'electric_bike', 'docked_bike']
    expected_datetime_format = '%Y-%m-%d %H:%M:%S'
    valid_user_types = ['member', 'casual']
    ride_id_col_name = 'ride_id'
    bike_type_col_name = 'rideable_type'
    start_time_col_name = 'started_at'
    end_time_col_name = 'ended_at'
    start_station_col_name = 'start_station_name'
    start_station_id_col_name = 'start_station_id'
    end_station_col_name = 'end_station_name'
    end_station_id_col_name = 'end_station_id'
    start_latitude_col_name = 'start_lat'
    start_longitude_col_name = 'start_lng'
    end_latitude_col_name = 'end_lat'
    end_longitude_col_name = 'end_lng'
    user_type_col_name = 'member_casual'

