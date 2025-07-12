{{ config(
    materialized = 'incremental',
    unique_key    = 'region_key'
) }}
select 
region_key,
region_id,
region_name,
last_updated_date
from {{ ref('stg_region') }}

{% if is_incremental() %}
  where last_updated_date > (
    select coalesce(max(last_updated_date), '1900-01-01') from {{ this }}
  )
{% endif %}