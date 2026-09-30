-- GOLD fact table. Grain: one row per registered player.
-- No raw identifiers: player_id is salted and hashed; birth_year becomes an age band.

with regs as (
    select * from {{ ref('stg_player_registrations') }}
),

bands as (
    select * from {{ ref('dim_age_band') }} where age_band_label != 'Unknown'
),

known_campaigns as (
    select campaign_id from {{ ref('campaigns') }}
),

prepared as (
    select
        r.*,
        year(r.registered_at) - r.birth_year as age_at_registration,
        case
            when r.campaign_id is null            then '__organic__'
            when kc.campaign_id is null           then '__unknown__'
            else r.campaign_id
        end as campaign_member
    from regs r
    left join known_campaigns kc on r.campaign_id = kc.campaign_id
)

select
    sha2(concat(p.player_id, '{{ env_var("PLAYER_HASH_SALT") }}'), 256)         as player_key,
    cast(date_format(p.registration_date, 'yyyyMMdd') as int)                   as date_key,
    {{ dbt_utils.generate_surrogate_key(['p.platform_code']) }}                 as platform_key,
    {{ dbt_utils.generate_surrogate_key(['p.country_code']) }}                  as country_key,
    {{ dbt_utils.generate_surrogate_key(["coalesce(b.age_band_label, 'Unknown')"]) }} as age_band_key,
    {{ dbt_utils.generate_surrogate_key(['p.campaign_member']) }}               as channel_key,
    p.registration_date,
    p.consent_analytics
from prepared p
left join bands b
    on p.age_at_registration between b.min_age and b.max_age
