select
    meteorological_observatory_name
    , large_area_code
    , large_area_name
from `{{ project_id }}.{{ setting_datasetname }}.{{ setting_table_view_names.largearea }}`
;