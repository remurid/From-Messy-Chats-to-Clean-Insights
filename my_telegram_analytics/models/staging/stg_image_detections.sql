-- models/staging/stg_image_detections.sql

with source as (

    select * from {{ source('raw_enrichment', 'image_detections') }}

),

renamed as (

    select
        id as detection_id,
        message_id,
        image_path,
        detected_object_class,
        confidence_score,
        bounding_box,
        loaded_at

    from source

)

select * from renamed
