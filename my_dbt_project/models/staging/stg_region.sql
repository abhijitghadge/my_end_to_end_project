select
  region_id,
  region_name
from {{ ref('region') }}