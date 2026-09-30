-- One row the dashboard reads to show "data as of" and whether checks passed.
-- It only rebuilds when everything upstream (including tests) succeeded,
-- so built_at is effectively the time of the last verified publish.
select
    max(f.registration_date)                       as data_as_of_date,
    (select max(registered_at)
       from {{ ref('stg_player_registrations') }}) as latest_registration_at,
    count(*)                                       as total_players,
    current_timestamp()                            as built_at
from {{ ref('fct_player_registrations') }} f
