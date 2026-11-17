{{ config(materialized='view') }}
select record_key,pickup,dropoff,distance,pickup_zone,dropoff_zone,fare,total,payment_type,partition_month,
  date_diff('second',pickup,dropoff)/60.0 duration_minutes
from {{ source('raw','trips') }}
