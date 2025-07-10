-- tests/no_negative_sales.sql

select
  sale_id,
  sale_amount
from {{ ref('fct_sales') }}
where sale_amount < 0