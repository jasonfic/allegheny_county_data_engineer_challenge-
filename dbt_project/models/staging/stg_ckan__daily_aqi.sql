{{
  config(
    materialized='incremental',
    unique_key='record_id'
  )
}}

with source as (
    select * from {{ source('raw_ckan', 'daily_aqi') }}
),

renamed as (
    select
        cast(_id as integer) as record_id,
        cast("date" as date) as measurement_date,
        trim(cast(site as varchar)) as site_name,
        trim(cast(parameter as varchar)) as metric_parameter,
        cast(index_value as double) as index_value,
        trim(cast(description as varchar)) as aqi_category,
        trim(cast(health_advisory as varchar)) as health_advisory,
        trim(cast(health_effects as varchar)) as health_effects,
        cast(_extracted_at as timestamp) as extracted_at
    from source
)

select * from renamed

{% if is_incremental() %}
    where extracted_at >= (select coalesce(max(extracted_at), min(measurement_date)) from {{ this }})
{% endif %}