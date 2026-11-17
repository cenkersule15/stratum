select z.zone_id,z.zone_name,z.borough,count(*) trips,sum(t.total)::decimal(18,2) revenue,
  round(avg(t.distance),2) avg_distance
from {{ ref('stg_trips') }} t join {{ source('raw','zones') }} z on t.pickup_zone=z.zone_id
group by 1,2,3
