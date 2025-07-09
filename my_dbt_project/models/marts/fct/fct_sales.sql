{{ config(
    materialized = 'incremental',
    unique_key    = 'sale_id'
) }}

with sales as (
    select * from {{ ref('stg_sales') }}
)

select
  s.sale_id,
  c.customer_name,
  r.region_name,
  s.product_id,
  s.sale_amount,
  s.sale_date
from sales s
left join {{ ref('dim_customer') }} c using (customer_id)
left join {{ ref('dim_region') }}   r using (region_id)

{% if is_incremental() %}
  where s.sale_date > (
    select coalesce(max(sale_date), '1900-01-01') from {{ this }}
  )
{% endif %}
