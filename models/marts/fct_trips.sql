with

    source as (select * from {{ ref("int_fact_trips_02") }}),

    trans_01 as (

        select
            bike_id,
            start_time,
            dim_date_id_trip,
            scd_dim_weather_id,
            stop_time,
            round(trip_duration_seconds / 60,0) as trip_duration_min,
            trip_duration,
            period_of_day,
            trip_distance_meters,
            case
                when trip_distance_meters between 0 and 1000 then '0-1'
                when trip_distance_meters between 1001 and 2000 then '1-2'
                when trip_distance_meters between 2001 and 3000 then '2-3'
                when trip_distance_meters between 3001 and 4000 then '3-4'
                when trip_distance_meters between 4001 and 5000 then '4-5'
                else '5+'
            end as trip_distance_range,
            user_type,
            age,
            age_range,
            gender,
            birth_year,
            start_station_name,
            end_station_name,
            trip_duration_seconds,
            start_station_id,
            start_station_latitude,
            start_station_longitude,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location

        from source

    ),

    trans_02 as (

        select
            bike_id,
            start_time,
            dim_date_id_trip,
            scd_dim_weather_id,
            stop_time,
            trip_duration_min,
            case
                when trip_duration_min between 0 and 10 then '0-10'
                when trip_duration_min between 11 and 20 then '11-20'
                when trip_duration_min between 21 and 30 then '21-30'
                when trip_duration_min between 31 and 40 then '31-40'
                when trip_duration_min between 41 and 50 then '41-50'
                else '50+'
            end as trip_duration_min_range,
            trip_duration,
            period_of_day,
            trip_distance_meters,
            trip_distance_range,
            user_type,
            age,
            age_range,
            gender,
            birth_year,
            start_station_name,
            end_station_name,
            trip_duration_seconds,
            start_station_id,
            start_station_latitude,
            start_station_longitude,
            start_location,
            end_station_id,
            end_station_latitude,
            end_station_longitude,
            end_location

        from trans_01

    ),

    trans_03 as (

        select
            count(1) as trip_count,
            sum(trip_distance_meters) / 1000 as trip_distance_sum_km,
            dim_date_id_trip,
            scd_dim_weather_id,
            trip_duration_min_range,
            period_of_day,
            trip_distance_range,
            user_type,
            age_range,
            gender

        from trans_02

        group by 
            dim_date_id_trip,
            scd_dim_weather_id,
            trip_duration_min_range,
            period_of_day,
            trip_distance_range,
            user_type,
            age_range,
            gender

    )


select *
from trans_03
