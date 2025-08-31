-- models/trn/trn_insurance.sql
{{ config(
    materialized='incremental',
    schema='STAGING',
    alias='TRN_INSURANCE',
    unique_key='INSURANCE_ID',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        i.INSURANCE_ID,
        i.PATIENT_ID,
        i.PROVIDER,
        i.PLAN,
        i.UPDATED_AT
    FROM {{ source('STAGING', 'STG_INSURANCE') }} i
    INNER JOIN {{ ref('trn_patient') }} p
        ON i.PATIENT_ID = p.PATIENT_ID
)

SELECT * FROM source_data

{% if is_incremental() %}
  WHERE UPDATED_AT > (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}
