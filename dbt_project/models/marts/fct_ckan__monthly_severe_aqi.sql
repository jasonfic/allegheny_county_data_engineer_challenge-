with daily_summary as (
    select * from {{ ref('int_ckan__county_aqi_summary') }}
)

select
    measurement_month,
    count(*) as total_days_in_month,
    count(case when is_unsafe_day then 1 end) as unsafe_day_count,
    max(county_max_aqi) as peak_monthly_aqi,
    -- Top pollutant that drove the most peak days in that month
    mode(critical_parameter) as dominant_critical_parameter
from daily_summary
group by measurement_month
order by unsafe_day_count desc, peak_monthly_aqi desc