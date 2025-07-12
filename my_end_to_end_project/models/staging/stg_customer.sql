select 
sha2(customerid) as customer_key,
customerid as customer_id,
customername as customer_name,
last_updated_date
from {{ source('raw','customer') }}