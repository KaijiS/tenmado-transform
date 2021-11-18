
select
    count(*) as cnt
from `{{ project_id }}.{{ import_datasetname }}.{{ table_name }}`
where
    report_datetime = '{{ target_date }}'
;