{{ config(
    materialized = 'incremental',
    unique_key    = 'fact_sales_key'
) }}
select 
fact_sales_key,   
sale_key, 
customer_key, 
product_key,
region_key,
sale_date_key, 
sale_date,     
sale_amount,
last_updated_date
from {{ ref('stg_sales') }}

{% if is_incremental() %}
  where last_updated_date > (
    select coalesce(max(last_updated_date), '1900-01-01') from {{ this }}
  )
{% endif %}