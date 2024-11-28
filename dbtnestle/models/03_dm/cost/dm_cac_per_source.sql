SELECT 
    acquisition_month AS cac_period,
    total_n_customer AS new_customers,
    total_marketing_costs AS total_cost,
    total_expected_revenue,
    marketing_source,
     cac_ratio_per_source,
    CURRENT_TIMESTAMP AS _dm_datetime
FROM 
{{ref('fct_cac_per_source')}}
