insert into `{{ project_id }}.{{ interim_datasetname }}.{{ interim_table_names.week_weather_fillna }}`

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
,
-- large_area_codeとsmall_area_codeを紐付けるためにcityを付与する
week_weather_with_city as (
    select
        l.get_datetime
        , l.report_datetime
        , l.meteorological_observatory_name
        , l.large_area_code
        , l.large_area_name
        , l.forecast_target_date
        , l.weather_code
        , l.pop
        , l.reliability
        , r.city_code
        , r.city_name
    from week_weather l
    left outer join `{{ project_id }}.{{ setting_datasetname }}.{{ setting_table_view_names.largearea_city }}` r
        using (meteorological_observatory_name, large_area_code, large_area_name)
)

,
tomorrow_pops as (
    select
        get_datetime
        , report_datetime
        , meteorological_observatory_name
        , small_area_code
        , small_area_name
        , forecast_target_date
        , pops0006
        , pops0612
        , pops1218
        , pops1824
    from `{{ project_id }}.{{ import_datasetname }}.{{ import_table_names.tomorrow_pops }}`
    where
        report_datetime between '{{ yesterday }} 00:00:00' and '{{ yesterday }} 23:59:59'
)
,
tomorrow_pops_with_city as (
    select
        l.get_datetime
        , l.report_datetime
        , l.meteorological_observatory_name
        , l.small_area_code
        , l.small_area_name
        , l.forecast_target_date
        , cast(l.pops0006 as string) as pops0006
        , cast(l.pops0612 as string) as pops0612
        , cast(l.pops1218 as string) as pops1218
        , cast(l.pops1824 as string) as pops1824
        , r.city_code
        , r.city_name
    from tomorrow_pops l
    left outer join `{{ project_id }}.{{ setting_datasetname }}.{{ setting_table_view_names.smallarea_city }}` r
        using (meteorological_observatory_name, small_area_code, small_area_name)

)

select
    l.get_datetime
    , l.report_datetime
    , l.meteorological_observatory_name
    , l.large_area_code
    , l.large_area_name
    , l.forecast_target_date
    , l.weather_code
    , case
        when l.pop is not null then cast(l.pop as string)
        else r.pops0006 || '/' || r.pops0612 || '/' || r.pops1218 || '/' || r.pops1824
    end as pop
    , l.reliability
from week_weather_with_city l
left outer join tomorrow_pops_with_city r
    using(meteorological_observatory_name, forecast_target_date, city_code, city_name)
;

