-- models/trn/trn_health_condition.sql
{{ config(
    materialized='incremental',
    schema='STAGING',
    alias='TRN_HEALTH_CONDITION',
    unique_key='CONDITION_ID',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        hc.CONDITION_ID,
        hc.PATIENT_ID,
        hc.CONDITION_NAME,
        hc.DIAGNOSIS_DATE,
        hc.UPDATED_AT
    FROM {{ source('STAGING', 'STG_HEALTH_CONDITION') }} hc
    INNER JOIN {{ ref('trn_patient') }} p
        ON hc.PATIENT_ID = p.PATIENT_ID
)

SELECT * FROM source_data

{% if is_incremental() %}
  WHERE UPDATED_AT > (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}
