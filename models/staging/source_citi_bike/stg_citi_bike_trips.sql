--https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime-transact-sql?view=fabric

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
            [Bike ID] AS bike_id,
            cast([Start Time] as datetime) AS start_time,
            [Stop Time] AS stop_time,
            [Trip Duration] AS trip_duration,
            [Start Station ID] AS start_station_id,
            [Start Station Name] AS start_station_name,
            cast([Start Station Latitude] as decimal) AS start_station_latitude,
            [Start Station Longitude] AS start_station_longitude,
            'POINT(' + [Start Station Longitude] + ' ' + [Start Station Latitude] + ')' as start_location,
            [End Station ID] AS end_station_id,
            [End Station Name] AS end_station_name,
            [End Station Latitude] AS end_station_latitude,
            [End Station Longitude] AS end_station_longitude,
            'POINT(' + [End Station Longitude] + ' ' + [End Station Latitude] + ')' as end_location,
            [User Type] AS user_type,
            [Birth Year] AS birth_year,
            [Gender] AS gender,
            [metadata_filename] AS metadata_filename,
            null AS metadata_file_row_number,
            [metadata_file_last_modified] AS metadata_file_last_modified,
            --to_varchar([Start Time], 'YYYYMM') as start_time_year_month
            convert(nvarchar(MAX), [Start Time], 112) as start_time_year_month

        from source
    ),

    add_new_columns as (
        select
            bike_id,
            start_time,
            stop_time,
            trip_duration,
            start_station_id,
            start_station_name,
            start_station_latitude,
            start_station_longitude,
            start_location,
            end_station_id,
            end_station_name,
            end_station_latitude,
            end_station_longitude,
            end_location,
            user_type,
            birth_year,
            gender,
            metadata_filename,
            metadata_file_row_number,
            metadata_file_last_modified,
            start_time_year_month

        from convert_datatypes

        where [start_station_latitude] > 0 
        and [start_time] between {{ last_extracted_date }} and dateadd(day, {{ env_var('DBT_EXTRACTION_WINDOW_IN_DAYS') }}, {{ last_extracted_date }})
    ),

    incremental_logic as (
        select
            bike_id,
            start_time,
            stop_time,
            trip_duration,
            start_station_id,
            start_station_name,
            start_station_latitude,
            start_station_longitude,
            start_location,
            'ST_GeographyFromText(start_location)' as start_location_geography,
            end_station_id,
            end_station_name,
            end_station_latitude,
            end_station_longitude,
            end_location,
            'ST_GeographyFromText(end_location)' as end_location_geography,
            user_type,
            birth_year,
            gender,
            metadata_filename,
            metadata_file_row_number,
            metadata_file_last_modified,
            start_time_year_month
        from add_new_columns
    )


select *
from incremental_logic
