select cast(pickup as date) as day,partition_month,count(*) trips,
  sum(total)::decimal(18,2) revenue,round(avg(distance),2) avg_distance,
  round(avg(duration_minutes),2) avg_duration
from {{ ref('stg_trips') }} group by 1,2
