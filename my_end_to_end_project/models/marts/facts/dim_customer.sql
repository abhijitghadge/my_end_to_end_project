{{ config(
    materialized = 'incremental',
    unique_key    = 'customer_key'
) }}
select 
customer_key,
customer_id,
customer_name,
last_updated_date
from {{ ref('stg_customer') }}

{% if is_incremental() %}
  where last_updated_date > (
    select coalesce(max(last_updated_date), '1900-01-01') from {{ this }}
  )
{% endif %}