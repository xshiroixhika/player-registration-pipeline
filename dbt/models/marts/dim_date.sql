with spine as (
    select explode(sequence(
        to_date('{{ var("date_spine_start") }}'),
        date_add(current_date(), 365),
        interval 1 day
    )) as date_day
)
select
    cast(date_format(date_day, 'yyyyMMdd') as int) as date_key,
    date_day,
    date_trunc('week', date_day)                   as week_start,
    date_format(date_day, 'yyyy-MM')               as year_month,
    quarter(date_day)                              as quarter,
    year(date_day)                                 as year,
    dayofweek(date_day) in (1, 7)                  as is_weekend
from spine
