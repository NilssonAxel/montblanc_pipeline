{{ config(materialized='incremental', unique_key='date') }}

with dates as (
    {% if is_incremental() %}
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="(select dateadd(day, 1, max(date)) from {{ this }})",
            end_date="cast('2030-01-01' as date)"
        ) }}
    {% else %}
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2020-01-01' as date)",
            end_date="cast('2030-01-01' as date)"
        ) }}
    {% endif %}
),

final as (
    select
        cast(date_day as date)  as date,
        year(date_day)          as year,
        month(date_day)         as month,
        date_format(date_day, 'MMMM') as month_name,
        quarter(date_day)       as quarter,
        dayofweek(date_day)     as day_of_week,
        date_format(date_day, 'EEEE') as day_name,
        dayofweek(date_day) in (1, 7) as is_weekend,
        month(date_day) between 6 and 9 as is_climbing_season
    from dates
)

select * from final
