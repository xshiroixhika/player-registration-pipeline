-- GOLD aggregate read by Tableau.
-- Grain: date x country x platform x age band x channel.
-- Small cells are suppressed so individuals can't be re-identified.

with joined as (
    select
        f.registration_date,
        c.region,
        c.country_name,
        p.platform_family,
        p.platform_name,
        a.age_band_label,
        a.sort_order as age_band_sort,
        ch.channel
    from {{ ref('fct_player_registrations') }} f
    join {{ ref('dim_country') }}             c  on f.country_key  = c.country_key
    join {{ ref('dim_platform') }}            p  on f.platform_key = p.platform_key
    join {{ ref('dim_age_band') }}            a  on f.age_band_key = a.age_band_key
    join {{ ref('dim_acquisition_channel') }} ch on f.channel_key  = ch.channel_key
),

counted as (
    select
        registration_date, region, country_name, platform_family, platform_name,
        age_band_label, age_band_sort, channel,
        count(*) as raw_count
    from joined
    group by all
)

select
    registration_date, region, country_name, platform_family, platform_name,
    age_band_label, age_band_sort, channel,
    case when raw_count >= {{ var('min_cell_size') }} then raw_count end as registration_count,
    raw_count < {{ var('min_cell_size') }}                               as is_suppressed
from counted
