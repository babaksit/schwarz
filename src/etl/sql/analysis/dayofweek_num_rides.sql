select date_format(started_at, "E") as day_of_week, count(ride_id) as num_rides
from citibike_weather_merged
group by day_of_week
order by date_format(started_at, "E")