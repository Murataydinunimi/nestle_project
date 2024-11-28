{% macro get_datalake_model(source_name, table_name) %}
WITH new_data AS (
    SELECT
        *,
        HASH(OBJECT_CONSTRUCT(*)) AS _row_hash
    FROM {{ source(source_name, table_name) }} AS raw
)

SELECT *,
CURRENT_TIMESTAMP AS _dl_datetime
FROM new_data

{% if is_incremental() %}
    WHERE _row_hash NOT IN (SELECT _row_hash FROM {{ this }})
{% endif %}
{% endmacro %}
