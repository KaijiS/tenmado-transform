
select
    count(*) as cnt
from `{{ project_id }}.{{ import_datasetname }}.{{ table_name }}`
where
    report_datetime between '{{ yesterday }} 00:00:00' and '{{ today }} 23:59:59'
;