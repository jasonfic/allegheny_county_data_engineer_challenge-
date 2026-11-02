with staging as (
    select * from {{ ref('stg_ckan__daily_aqi') }}
),

daily_readings as (
    select
        measurement_date,
        site_name,
        metric_parameter,
        index_value,
        aqi_category
    from staging
    where index_value is not null
),

daily_county_rollup as (
    select
        measurement_date,
        strftime(measurement_date, '%Y-%m') as measurement_month,
        
        -- Maximum Pollutant Principle: Combined County AQI, site-independent
        max(index_value) as county_max_aqi,
        
        -- Identify the worst-hit station and driving pollutant using DuckDB's arg_max
        arg_max(site_name, index_value) as critical_measurement_site_name,
        arg_max(metric_parameter, index_value) as critical_parameter,
        arg_max(aqi_category, index_value) as county_aqi_category,

        -- Target threshold flag: Unhealthy-Hazardous range (150+)
        case 
            when max(index_value) > 150 then true 
            else false 
        end as is_unsafe_day,

        -- Spatial extent context: how localized was the event?
        count(distinct site_name) as total_reporting_sites,
        count(distinct case when index_value > 150 then site_name end) as sites_in_unsafe_range

    from daily_readings
    group by measurement_date
)

select *
from daily_county_rollup