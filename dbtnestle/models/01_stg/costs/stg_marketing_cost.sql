WITH stg_mkt AS (
   SELECT
   DATE AS YEAR_MONTH,
   MARKETING_SOURCE,
   MARKETING_COSTS,
   CURRENT_TIMESTAMP AS _stg_datetime

FROM {{ref('dl_marketing_cost')}})

SELECT * FROM stg_mkt
