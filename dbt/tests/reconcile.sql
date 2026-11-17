select 'row count mismatch' failure
where (select coalesce(sum(trips),0) from {{ ref('daily_metrics') }})
   != (select count(*) from {{ ref('stg_trips') }})
union all
select 'revenue mismatch' where
 (select coalesce(sum(revenue),0) from {{ ref('daily_metrics') }})
 != (select coalesce(sum(total),0) from {{ ref('stg_trips') }})
