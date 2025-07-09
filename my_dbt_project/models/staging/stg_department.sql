-- models/staging/stg_department.sql

with raw as (
  select * from {{ ref('department') }}
)

select
  raw.department_id,
  raw.department_name
from raw
