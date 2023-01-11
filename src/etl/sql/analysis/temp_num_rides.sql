with temp_min_max as (
    select min(cast(avg_temp as int)) as min,
           max(cast(avg_temp as int)) as max
      from citibike_weather_merged
),
bucketize as (
   select width_bucket(avg_temp, min, max, 9) as bucket,
       min(cast(avg_temp as int)) as min_temp,
       max(cast(avg_temp as int)) as max_temp,
          count(ride_id) as num_rides
     from citibike_weather_merged, temp_min_max
 group by bucket
 order by bucket
)
select bucket, min_temp, max_temp , num_rides
   from bucketize
