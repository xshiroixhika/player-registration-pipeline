-- Campaign-level dimension, plus explicit Organic and Unknown members so
-- every registration maps to a row (no facts lost to failed joins).
with campaigns as (
    select campaign_id, channel, partner from {{ ref('campaigns') }}
    union all
    select '__organic__', 'Organic', 'None'
    union all
    select '__unknown__', 'Unknown', 'Unknown'
)
select
    {{ dbt_utils.generate_surrogate_key(['campaign_id']) }} as channel_key,
    campaign_id,
    channel,
    partner
from campaigns
