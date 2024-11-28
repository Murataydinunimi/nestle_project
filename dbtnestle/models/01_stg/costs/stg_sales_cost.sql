WITH stg_sales AS (
  SELECT
	MONTH AS YEAR_MONTH,
	TOTAL_SALES_COSTS, 
	TRIAL_COSTS 
FROM {{ref('dl_sales_cost')}}
)

SELECT * FROM stg_sales
