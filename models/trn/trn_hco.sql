-- models/trn/trn_hco.sql
{{ config(
    materialized='incremental',
    schema='STAGING',
    alias='TRN_HCO',
    unique_key='HCO_ID',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        hco.HCO_ID,
        hco.PATIENT_ID,
        hco.NAME AS HCO_NAME,
        hco.LOCATION,
        hco.UPDATED_AT
    FROM {{ source('STAGING', 'STG_HCO') }} hco
    INNER JOIN {{ ref('trn_patient') }} p
        ON hco.PATIENT_ID = p.PATIENT_ID
)

SELECT * FROM source_data

{% if is_incremental() %}
  WHERE UPDATED_AT > (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}
