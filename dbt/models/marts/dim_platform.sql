select
    {{ dbt_utils.generate_surrogate_key(['platform_code']) }} as platform_key,
    platform_code,
    platform_name,
    platform_family
from {{ ref('platforms') }}
