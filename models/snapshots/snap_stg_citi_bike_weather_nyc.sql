{{ custom_snapshot(
    source_model_name = 'stg_citi_bike_weather_nyc',
    key_col = 'time_readable',
    other_cols = [
        'country',
        'city_name',
        'weather_main',
        'weather_description',
        'temperature',
        'humidity',
        'wind_speed',
        'city_latitude',
        'city_longitude',
        'city_location',
        'pressure',
        'wind_deg',
        'city_id',
        'city_findname'
	],
    timestamp_col = 'metadata_file_last_modified'
) }}