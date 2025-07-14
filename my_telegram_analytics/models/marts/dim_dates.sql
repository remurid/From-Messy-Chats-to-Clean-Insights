-- models/marts/dim_dates.sql
-- This model creates a comprehensive date dimension table.

-- Generate a sequence of dates from the earliest message date to today
with date_series as (
    select 
        (select min(date(message_posted_at)) from {{ ref('stg_telegram_messages') }})::date + s.a as date_day
    from generate_series(0, 365*5) as s(a) -- Generate for 5 years
    where (select min(date(message_posted_at)) from {{ ref('stg_telegram_messages') }})::date + s.a <= current_date
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dow from date_day) as day_of_week,
    to_char(date_day, 'Day') as day_name,
    to_char(date_day, 'Month') as month_name,
    extract(week from date_day) as week_of_year,
    extract(quarter from date_day) as quarter_of_year
from date_series
