{{ config(
    materialized = 'incremental',
    unique_key    = 'sale_id',
    incremental_strategy   = 'merge'
) }}

select
  sale_id,
  customer_id,
  product_id,
  region_id,
  sale_amount,
  sale_date
from {{ ref('sales') }}

  {% if is_incremental() %}
    -- only pull sales newer than what’s already in the target table
    where sale_date > (
      select max(sale_date) 
      from {{ this }}
    )
  {% endif %}