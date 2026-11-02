/*
  Validates that every row's index_value falls strictly within
  the defined EPA AQI description thresholds, and flags nulls, negative values,
  or unmapped category descriptions.
*/

with records_to_validate as (
    select
        record_id,
        measurement_date,
        site_name,
        metric_parameter,
        index_value,
        aqi_category
    from {{ ref('stg_ckan__daily_aqi') }}
)

select
    record_id,
    measurement_date,
    site_name,
    metric_parameter,
    index_value,
    aqi_category,
    case
        when index_value is null then 'Null index value'
        when index_value < 0 then 'Negative index value'
        when aqi_category = 'Good' and index_value not between 0 and 50 then 'Out of Good range [0-50]'
        when aqi_category = 'Moderate' and index_value not between 51 and 100 then 'Out of Moderate range [51-100]'
        when aqi_category = 'Unhealthy for Sensitive Groups' and index_value not between 101 and 150 then 'Out of USG range [101-150]'
        when aqi_category = 'Unhealthy' and index_value not between 151 and 200 then 'Out of Unhealthy range [151-200]'
        when aqi_category = 'Very Unhealthy' and index_value not between 201 and 300 then 'Out of Very Unhealthy range [201-300]'
        when aqi_category = 'Hazardous' and index_value not between 301 and 500 then 'Out of Hazardous range [301-500]'
        else 'Unrecognized AQI category'
    end as failure_reason
from records_to_validate
where
    index_value is null
    or index_value < 0
    or case aqi_category
        when 'Good'                           then index_value not between 0 and 50
        when 'Moderate'                       then index_value not between 51 and 100
        when 'Unhealthy for Sensitive Groups' then index_value not between 101 and 150
        when 'Unhealthy'                      then index_value not between 151 and 200
        when 'Very Unhealthy'                 then index_value not between 201 and 300
        when 'Hazardous'                      then index_value not between 301 and 500
        else true -- Flags any unmapped or malformed category names
    end