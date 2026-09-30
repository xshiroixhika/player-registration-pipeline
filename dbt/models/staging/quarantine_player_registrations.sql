{{ config(materialized='table') }}

-- Rows that failed validation, with a reason, so someone can investigate.
select
    case
        when player_id is null  then 'missing_player_id'
        when created_at is null then 'missing_created_at'
    end as quarantine_reason,
    *
from {{ source('bronze', 'raw_player_registrations') }}
where (player_id is null or created_at is null)
  and _ingested_at >= current_date() - interval 30 days
