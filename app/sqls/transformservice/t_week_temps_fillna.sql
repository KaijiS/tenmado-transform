insert into `{{ project_id }}.{{ interim_datasetname }}.{{ interim_table_names.week_temps_fillna }}`

with

week_temps as (
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
    from `{{ project_id }}.{{ import_datasetname }}.{{ import_table_names.week_temps }}`
    where
        report_datetime between '{{ yesterday }} 00:00:00' and '{{ yesterday }} 23:59:59'
)

,
tomorrow_temps as (
    select
        get_datetime
        , report_datetime
        , meteorological_observatory_name
        , city_code
        , citya_name as city_name
        , forecast_target_date
        , lowest_temperature
        , highest_temperature
    from `{{ project_id }}.{{ import_datasetname }}.{{ import_table_names.tomorrow_temps }}`
    where
        report_datetime between '{{ yesterday }} 00:00:00' and '{{ yesterday }} 23:59:59'
)

select
    l.get_datetime
    , l.report_datetime
    , l.meteorological_observatory_name
    , l.city_code
    , l.city_name
    , l.forecast_target_date
    , case
        when l.lowest_temperature is not null then l.lowest_temperature
        else r.lowest_temperature
    end as lowest_temperature
    , lowest_temperature_upper
    , lowest_temperature_lower
    , case
        when l.highest_temperature is not null then l.highest_temperature
        else r.highest_temperature
    end as highest_temperature
    , highest_temperature_upper
    , highest_temperature_lower
from week_temps l
left outer join tomorrow_temps r
    using(meteorological_observatory_name, forecast_target_date, city_code, city_name)
;

