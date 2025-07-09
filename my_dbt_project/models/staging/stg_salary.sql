select
  employee_id,
  salary_id,
  salary_amount,
  to_date(effective_date) as effective_date
from {{ ref('salary') }}