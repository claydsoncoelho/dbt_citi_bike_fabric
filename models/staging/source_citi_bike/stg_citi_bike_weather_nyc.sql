{{ config(
    materialized='incremental',
    unique_key='time_readable'
) }}

with

    source as (select * from {{ source("source_citi_bike", "weather_nyc") }}),

    filter_incremental as (

        select
            cast(TIME_READABLE as datetime2(6)) as time_readable,
			CITY_NAME as city_name,
			COUNTRY as country,
			cast(CITY_ID as int) as city_id,
			CITY_FINDNAME as city_findname,
			cast(CITY_LATITUDE as float) as city_latitude,
			cast(CITY_LONGITUDE as float) as city_longitude,
			CITY_LOCATION as city_location,
			WEATHER_DESCRIPTION as weather_description,
			WEATHER_MAIN as weather_main,
			cast(TEMPERATURE as float) as temperature,
			cast(HUMIDITY as int) as humidity,
			cast(PRESSURE as float) as pressure,
			cast(WIND_SPEED as float) as wind_speed,
			cast(WIND_DEG as float) as wind_deg,
			METADATA_FILENAME as metadata_filename,
			cast(METADATA_FILE_ROW_NUMBER as int) as metadata_file_row_number,
			METADATA_FILE_LAST_MODIFIED as metadata_file_last_modified

        from source 

        {% if incremental %}
            where source.time_readable >= {{ this }}.time_readable
        {% endif %}

    ),
    

    identify_duplicated as (

        select
            _inner.*,
            row_number() over (
                partition by time_readable
                order by metadata_filename
            ) as rn
        from filter_incremental as _inner

    ),

    remove_duplicated as (

        select distinct 
            data.time_readable,
			data.city_name,
			data.country,
			data.city_id,
			data.city_findname,
			data.city_latitude,
			data.city_longitude,
			data.city_location,
			data.weather_description,
			data.weather_main,
			data.temperature,
			data.humidity,
			data.pressure,
			data.wind_speed,
			data.wind_deg,
			data.metadata_filename,
			data.metadata_file_row_number,
			data.metadata_file_last_modified

        from identify_duplicated as data
        where data.rn = 1

    )


select *
from remove_duplicated
