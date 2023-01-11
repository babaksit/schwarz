WITH total_members AS (SELECT COUNT(*) as total FROM citibike_weather_merged)
SELECT member_casual as user_type, COUNT(*) / (SELECT total FROM total_members) * 100 AS percentage
FROM citibike_weather_merged
GROUP BY user_type;