select
  transaction_id,
  customer_id,
  product_id,
  quantity,
  cast(amount as numeric(12,2)) as amount,
  region,
  cast(timestamp as timestamp) as ts,
  date_trunc('day', cast(timestamp as timestamp))::date as order_date
from {{ source('public','sales') }}