select
    {{ dbt_utils.generate_surrogate_key(['country_code']) }} as country_key,
    country_code,
    country_name,
    region
from {{ ref('country_codes') }}
