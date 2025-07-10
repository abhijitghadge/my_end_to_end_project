{{ config(
    materialized = 'incremental',
    unique_key    = 'sale_id'
) }}

select
  sale_id,
  customer_id,
  product_id,
  region_id,
  sale_amount,
  sale_date
from {{ ref('sales') }}