-- models/marts/dim_channels.sql
-- This model creates a dimension table for Telegram channels.

with channels as (
    select distinct
        channel_id
    from {{ ref('stg_telegram_messages') }}
    where channel_id is not null
)

select
    channel_id,
    -- In a real scenario, you might join this with another source
    -- to get the channel name, but for now, the ID is the key.
    'Channel ' || channel_id::text as channel_name -- Placeholder name
from channels