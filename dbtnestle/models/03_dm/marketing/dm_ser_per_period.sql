SELECT 
year_month,
total_expected_revenue_per_period,
all_cost,
ser_ratio,
CURRENT_TIMESTAMP as _dm_datetime
 FROM {{ref('fct_ser_per_period')}}