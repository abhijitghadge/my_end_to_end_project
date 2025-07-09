-- tests/no_negative_sales.sql

select
  sale_id,
  quantity
from {{ ref('fct_sales') }}
where quantity < 0
