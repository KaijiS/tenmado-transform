select
    r.meteorological_observatory_name
    , l.meteorological_observatory_name
    , l.large_area_code
    , l.large_area_name
from `{{ project_id }}.{{ setting_datasetname }}.{{ setting_table_view_names.largearea }}` l
inner join `{{ project_id }}.{{ setting_datasetname }}.{{ setting_table_view_names.meteorologicalobservatory }}` r
    using(meteorological_observatory_name)
;