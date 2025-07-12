select 
sha2(regionid) region_key,
regionid as region_id,
regionname as region_name,
last_updated_date
from {{ source('raw','region') }}