{# 
    Reference: 
        https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=fabric

    Using datetime2(6) instead of datetime because of a known bug:
        https://learn.microsoft.com/en-us/fabric/data-warehouse/data-types
#}

{# Getting wartermark... #}
{%- set last_extracted_date -%}
    '{{ get_watermark(
        database_name='WH_Silver_Tech_One', 
        table_name='int_fact_trips_02', 
        column_name='start_time', 
        default_value='2017-01-01'
    ) }}'
{%- endset -%}

with
    source as (select * from {{ source("source_citi_bike", "trips") }}),

    convert_datatypes as (
        select
            cast([Bike ID] as int) AS bike_id,
            cast([Start Time] as datetime2(6)) AS start_time,
            cast([Stop Time] as datetime2(6)) AS stop_time,
            cast([Trip Duration] as int) AS trip_duration,
            cast([Start Station ID] as int) AS start_station_id,
            cast([Start Station Name] as varchar) AS start_station_name,
            cast([Start Station Latitude] as float) AS start_station_latitude,
            cast([Start Station Longitude] as float) AS start_station_longitude,
            cast([End Station ID] as int) AS end_station_id,
            cast([End Station Name] as varchar) AS end_station_name,
            cast([End Station Latitude] as float) AS end_station_latitude,
            cast([End Station Longitude] as float) AS end_station_longitude,
            cast([User Type] as varchar) AS user_type,
            cast([Birth Year] as decimal) AS birth_year,
            cast([Gender] as varchar) AS gender,
            cast([metadata_filename] as varchar) AS metadata_filename,
            cast([metadata_file_last_modified] as datetime2(6)) AS metadata_file_last_modified

        from source
    ),

    add_new_columns as (
        select
            cast(bike_id as varchar) + '|' + cast(start_time as varchar) as citi_bike_trip_id, 
            bike_id,
            start_time,
            stop_time,
            trip_duration,
            start_station_id,
            start_station_name,
            start_station_latitude,
            start_station_longitude,
            end_station_id,
            end_station_name,
            end_station_latitude,
            end_station_longitude,
            user_type,
            birth_year,
            gender,
            metadata_filename,
            metadata_file_last_modified

        from convert_datatypes

        where [start_station_latitude] > 0 
        and [start_time] between {{ last_extracted_date }} and dateadd(day, {{ env_var('DBT_EXTRACTION_WINDOW_IN_DAYS') }}, {{ last_extracted_date }})
    )


select *
from add_new_columns
