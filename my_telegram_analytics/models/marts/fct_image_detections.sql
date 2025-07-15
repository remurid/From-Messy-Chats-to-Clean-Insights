-- models/marts/fct_image_detections.sql

with detections as (

    select * from {{ ref('stg_image_detections') }}

),

messages as (

    select 
        message_id,
        channel_id,
        date_day
    from {{ ref('fct_messages') }}

)

select
    -- Primary Key
    detections.detection_id,

    -- Foreign Keys
    detections.message_id,
    messages.channel_id,
    messages.date_day,

    -- Detection Details
    detections.detected_object_class,
    detections.confidence_score,
    detections.bounding_box,
    detections.image_path

from detections
left join messages on detections.message_id = messages.message_id
