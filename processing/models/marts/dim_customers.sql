select
  customer_id,
  name,
  email,
  region,
  join_date
from {{ ref('stg_customers') }}