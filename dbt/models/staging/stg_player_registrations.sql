{{ config(
    materialized='incremental',
    unique_key='player_id',
    incremental_strategy='merge',
    partition_by=['registration_date']
) }}

-- SILVER: one clean, standardized row per real player.

with source as (

    select * from {{ source('bronze', 'raw_player_registrations') }}
    {% if is_incremental() %}
    -- Only new bronze data, plus a lookback window for late-arriving rows.
    where _ingested_at >= (
        select max(_bronze_ingested_at) - interval {{ var('lookback_days') }} days
        from {{ this }}
    )
    {% endif %}

),

valid as (
    -- Invalid rows are routed to quarantine_player_registrations, not silently lost.
    select * from source
    where player_id is not null
      and created_at is not null
),

deduped as (
    select * from valid
    qualify row_number() over (
        partition by player_id
        order by created_at, _ingested_at
    ) = 1
),

countries as (
    select * from {{ ref('country_codes') }}
),

standardized as (

    select
        cast(d.player_id as string)                          as player_id,
        cast(d.created_at as timestamp)                      as registered_at,
        cast(cast(d.created_at as timestamp) as date)        as registration_date,

        case
            when lower(trim(d.platform)) in ('pc', 'windows', 'steam', 'epic')       then 'pc'
            when lower(trim(d.platform)) in ('ps5', 'playstation', 'playstation 5')  then 'ps5'
            when lower(trim(d.platform)) in ('xbox', 'xsx', 'xss', 'xbox series x')  then 'xbox'
            when lower(trim(d.platform)) in ('switch', 'nintendo switch')            then 'switch'
            when lower(trim(d.platform)) in ('ios', 'iphone', 'ipad')                then 'ios'
            when lower(trim(d.platform)) = 'android'                                 then 'android'
            else 'unknown'
        end                                                  as platform_code,

        -- Accept either 2- or 3-letter codes; anything unrecognized becomes XX (Unknown).
        coalesce(c2.country_code, c3.country_code, 'XX')     as country_code,
        coalesce(lower(d.country_source), 'unknown')         as country_source,

        -- Impossible birth years are treated as missing rather than trusted.
        case
            when d.birth_year between 1900 and year(d.created_at) then cast(d.birth_year as int)
        end                                                  as birth_year,

        nullif(trim(d.campaign_id), '')                      as campaign_id,
        d.app_version,
        coalesce(cast(d.is_internal as boolean), false)      as is_internal,
        cast(d.consent_analytics as boolean)                 as consent_analytics,

        d._ingested_at                                       as _bronze_ingested_at,
        d._source_file                                       as _source_file,
        current_timestamp()                                  as _loaded_at

    from deduped d
    left join countries c2 on upper(trim(d.country_code)) = c2.country_code
    left join countries c3 on upper(trim(d.country_code)) = c3.alpha3

)

select * from standardized
where not is_internal   -- QA, employee, and test accounts never count as players
