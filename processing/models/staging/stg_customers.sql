select
  customer_id,
  name,
  email,
  region,
  cast(join_date as date) as join_date
from {{ source('public','customers') }}