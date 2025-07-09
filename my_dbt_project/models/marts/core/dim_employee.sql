{{ config(
    materialized = 'incremental',
    unique_key    = 'employee_id'
) }}

with emp as (
    select * from {{ ref('stg_employee') }}
),
sal as (
    select
      employee_id,
      salary_amount,
      effective_date
    from {{ ref('stg_salary') }}
)

select
  emp.employee_id,
  emp.first_name,
  emp.last_name,
  emp.department_id,
  sal.salary_amount,
  sal.effective_date as salary_effective_date
from emp
left join sal using (employee_id)

{% if is_incremental() %}
  -- only load new salary records
  where sal.effective_date > (
    select coalesce(max(salary_effective_date), '1900-01-01') from {{ this }}
  )
{% endif %}
