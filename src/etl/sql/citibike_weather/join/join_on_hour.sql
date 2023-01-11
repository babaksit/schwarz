SELECT c.*, w1.temp as start_temp, w2.temp as end_temp,
       ROUND(((w1.temp + w2.temp) / 2) ,2) AS avg_temp
FROM citibike_cleaned c
JOIN weather_cleaned w1 ON DATE_TRUNC('hour', c.started_at) = DATE_TRUNC('hour', w1.datetime)
JOIN weather_cleaned w2 ON DATE_TRUNC('hour', c.ended_at) = DATE_TRUNC('hour', w2.datetime)