select extract(hour from started_at) as hour, count(ride_id) as num_rides
from citibike_weather_merged
group by hour
order by hour asc