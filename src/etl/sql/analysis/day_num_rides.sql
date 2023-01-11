select extract(day from started_at) as day, count(ride_id) as num_rides
from citibike_weather_merged
group by day
order by day asc