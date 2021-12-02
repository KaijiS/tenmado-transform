insert into `{{ project_id }}.{{ interim_datasetname }}.{{ interim_table_names.week_weather_temps }}`

with

weather as (
    select
        get_datetime
        , report_datetime
        , meteorological_observatory_name
        , large_area_code
        , large_area_name
        , forecast_target_date
        , weather_code
        , pop
        , reliability
    from `{{ project_id }}.{{ interim_datasetname }}.{{ interim_table_names.week_weather_fillna }}`
    where
        report_datetime between '{{ yesterday }} 00:00:00' and '{{ yesterday }} 23:59:59'
)

,
temps as (
    select
        get_datetime
        , report_datetime
        , meteorological_observatory_name
        , city_code
        , city_name
        , forecast_target_date
        , lowest_temperature
        , lowest_temperature_upper
        , lowest_temperature_lower
        , highest_temperature
        , highest_temperature_upper
        , highest_temperature_lower
    from `{{ project_id }}.{{ interim_datasetname }}.{{ interim_table_names.week_temps_fillna }}`
    where
        report_datetime between '{{ yesterday }} 00:00:00' and '{{ yesterday }} 23:59:59'
)

select
    l.get_datetime
    , l.report_datetime
    , l.meteorological_observatory_name
    , l.large_area_code
    , l.large_area_name
    , c.city_code
    , c.city_name
    , l.forecast_target_date
    , l.weather_code
    , l.pop
    , l.reliability
    , r.lowest_temperature
    , r.lowest_temperature_upper
    , r.lowest_temperature_lower
    , r.highest_temperature
    , r.highest_temperature_upper
    , r.highest_temperature_lower
from weather l
left outer join `{{ project_id }}.{{ setting_datasetname }}.{{ setting_table_view_names.largearea_city }}` c
    using (meteorological_observatory_name, large_area_code, large_area_name)
inner join temps r
    using(meteorological_observatory_name, city_code, city_name, forecast_target_date)
;
