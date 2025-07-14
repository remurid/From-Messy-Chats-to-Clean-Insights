-- models/marts/fct_messages.sql
-- This model creates the main fact table for messages.

with messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channels as (
    select * from {{ ref('dim_channels') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
)

select
    -- Primary Key
    messages.message_id,

    -- Foreign Keys
    messages.channel_id,
    dates.date_day,

    -- Message Metrics and Details
    messages.message_text,
    length(messages.message_text) as message_length,
    messages.view_count,
    messages.has_photo,
    messages.message_posted_at

from messages
left join dates on date(messages.message_posted_at) = dates.date_day