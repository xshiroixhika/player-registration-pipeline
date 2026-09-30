select
    {{ dbt_utils.generate_surrogate_key(['age_band_label']) }} as age_band_key,
    age_band_label,
    min_age,
    max_age,
    is_minor,
    sort_order
from {{ ref('age_bands') }}
