select
    get_datetime
    , report_datetime
    , meteorological_observatory_name
    , large_area_code
    , large_area_name
    , city_code
    , city_name
    , forecast_target_date
    , weather_code
    , pop
    , reliability
    , lowest_temperature
    , lowest_temperature_upper
    , lowest_temperature_lower
    , highest_temperature
    , highest_temperature_upper
    , highest_temperature_lower
from `{{ project_id }}.{{ processed_datasetname }}.{{ processed_table_names.week_weather_temps }}`
where
    report_datetime between '{{ yesterday }} 00:00:00' and '{{ yesterday }} 23:59:59'