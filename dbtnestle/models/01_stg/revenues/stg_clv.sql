WITH stg_clv AS(

SELECT 
CONTRACT AS contract_length,
AVG_CLV AS avg_clv,
CURRENT_TIMESTAMP AS _stg_datetime
FROM {{ ref('dl_clv') }}
)

SELECT * FROM stg_clv