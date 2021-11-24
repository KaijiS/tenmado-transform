with

week_weather as (
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
    from `{{ project_id }}.{{ import_datasetname }}.{{ import_table_names.week_weather }}`
    where
        report_datetime between '{{ yesterday }} 00:00:00' and '{{ yesterday }} 23:59:59'
)


select
    l.get_datetime
    , l.report_datetime
    , l.meteorological_observatory_name
    , l.large_area_code
    , l.large_area_name
    , l.forecast_target_date
    , r.city_code
    , r.city_name
from week_weather l
full outer join `{{ project_id }}.{{ setting_datasetname }}.{{ setting_table_view_names.largearea_city }}` r
    using (meteorological_observatory_name, large_area_code, large_area_name)
where
    r.city_code is null
    or
    l.large_area_code is null
;