select
  s.transaction_id,
  s.order_date,
  s.ts as timestamp,
  s.customer_id,
  s.product_id,
  s.quantity,
  s.amount,
  s.region,
  p.category,
  p.product_name
from {{ ref('stg_sales') }} s
join {{ ref('stg_products') }} p using (product_id)
