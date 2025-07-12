select          
SHA2(saleid)||SHA2(customerid)||SHA2(productid)||SHA2(regionid)||SHA2(saledate) AS fact_sales_key,    
SHA2(saleid) AS sale_key, 
SHA2(customerid) as customer_key, 
SHA2(productid) as product_key,
SHA2(regionid) as region_key,
SHA2(saledate) as sale_date_key, 
saleid as sale_id,          
customerid as customer_id,      
productid as product_id,       
regionid  as region_id,        
saleamount as sale_amount,      
saledate as sale_date,        
last_updated_date as last_updated_date
from {{ source('raw','sales') }}