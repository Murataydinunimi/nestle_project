WITH lead_duration AS (

SELECT
    dimc.contact_id,
    dimc.customer_date,
    diml.create_date,
    dimc.contract_length,
    (dimc.customer_date-diml.create_date) AS diff_in_days,
    dimc._dwh_datetime 
FROM {{ref('dim_customers')}} dimc
LEFT JOIN {{ref('dim_lead')}} diml
on dimc.contact_id = diml.contact_id


)

,

lead_agg AS(

SELECT  
    CONTRACT_LENGTH,
    AVG(diff_in_days) AS average_lead_duration
    FROM lead_duration
    GROUP BY contract_length
)

SELECT * FROM lead_agg