{{ config(
    materialized = 'incremental',
    unique_key    = 'product_key'
) }}
select 
product_key,
product_id,
product_name,
last_updated_date
from {{ ref('stg_product') }}

{% if is_incremental() %}
  where last_updated_date > (
    select coalesce(max(last_updated_date), '1900-01-01') from {{ this }}
  )
{% endif %}