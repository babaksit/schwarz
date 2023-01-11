WITH total_rides AS (SELECT COUNT(*) as total FROM citibike_weather_merged)
SELECT rideable_type as bike_type, COUNT(*) / (SELECT total FROM total_rides) * 100 AS percentage
FROM citibike_weather_merged
GROUP BY bike_type;

