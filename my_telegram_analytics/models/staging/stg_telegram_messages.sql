-- models/staging/stg_telegram_messages.sql

-- This model selects from the raw JSON data and extracts key fields.
-- It performs basic cleaning, casting, and renaming of columns.

with source as (

    select * from {{ source('raw', 'messages') }}

),

renamed as (

    select
        -- Extract data using PostgreSQL's JSONB operators
        (raw_data ->> 'id')::bigint as message_id,
        (raw_data -> 'peer_id' ->> 'channel_id')::bigint as channel_id,
        (raw_data ->> 'date')::timestamp as message_posted_at,
        raw_data ->> 'message' as message_text,
        (raw_data ->> 'views')::integer as view_count,
        
        -- Extract sender information (assuming it's in the 'from_id' field)
        (raw_data -> 'from_id' ->> 'user_id')::bigint as sender_user_id,

        -- A flag to indicate if the message contains a photo
        (raw_data -> 'media' ->> '_') = 'MessageMediaPhoto' as has_photo,
        
        -- Metadata
        loaded_at

    from source

)

select * from renamed
