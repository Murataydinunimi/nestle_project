WITH stg_lead AS (
    SELECT
	CONTACT_ID,
	MARKETING_SOURCE,
	TO_DATE(CREATE_DATE, 'DD.MM.YYYY') AS CREATE_DATE,
	KNOWN_CITY,
	MESSAGE_LENGTH,
	TEST_FLAG,
    CURRENT_TIMESTAMP AS _stg_datetime
FROM {{ ref('dl_lead_dataset') }})

SELECT * FROM stg_lead
--WHERE TEST_FLAG = 'TRUE'
