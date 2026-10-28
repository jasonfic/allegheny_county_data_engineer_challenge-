with source as (
    select * from {{ source('raw_ckan', 'daily_aqi') }}
),

renamed as (
    select
        cast(_id as integer) as record_id,
        cast(_source_resource_id as varchar) as resource_id,
        cast(_extracted_at as timestamp) as extracted_at,
        -- cast and map your dataset-specific CKAN fields here
        * EXCLUDE(_id, _source_resource_id, _extracted_at)
    from source
)

select * from renamed