{{ config(severity='warn') }}
-- Doesn't block the build, but surfaces new bad records for investigation.
select quarantine_reason, count(*) as records
from {{ ref('quarantine_player_registrations') }}
where _ingested_at >= current_date() - interval 1 day
group by quarantine_reason
