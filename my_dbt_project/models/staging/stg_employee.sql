{{ config(
    materialized='view'
) }}

select
  employee_id,
  first_name,
  last_name,
  department_id,
  to_date(created_at) as created_at,
  to_date(updated_at) as updated_at
from {{ ref('employee') }}