-- Fails the build if the warehouse's daily counts drift from the source DB's
-- counts by more than the tolerance. Returning rows = failure.
with latest_source_counts as (
    select registration_date, registration_count as source_count
    from {{ source('bronze', 'source_daily_counts') }}
    qualify row_number() over (
        partition by registration_date order by _ingested_at desc
    ) = 1
),

warehouse_counts as (
    select registration_date, count(*) as warehouse_count
    from {{ ref('fct_player_registrations') }}
    group by registration_date
)

select
    s.registration_date,
    s.source_count,
    coalesce(w.warehouse_count, 0) as warehouse_count
from latest_source_counts s
left join warehouse_counts w using (registration_date)
where s.registration_date >= current_date() - interval {{ var('reconcile_days') }} days
  and s.registration_date <  current_date()
  and abs(s.source_count - coalesce(w.warehouse_count, 0))
      > greatest(1, s.source_count * {{ var('reconcile_tolerance') }})
