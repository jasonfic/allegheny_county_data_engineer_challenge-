with source as (
    select * from {{ source('raw_ckan', 'daily_aqi') }}
),

renamed as (
    select
        cast(_id as integer) as record_id,
        * EXCLUDE(_id, _full_text)
    from source
)

select * from renamed