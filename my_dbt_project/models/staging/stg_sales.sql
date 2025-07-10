select
  sale_id,
  customer_id,
  product_id,
  region_id,
  sale_amount,
  sale_date,
  sale_quantity
from {{ ref('sales') }}