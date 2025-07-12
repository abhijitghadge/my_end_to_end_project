select 
SHA2(productid) AS product_key,
productid as product_id ,
productname as product_name,
last_updated_date
from {{ source('raw','product') }}