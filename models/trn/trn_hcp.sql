-- models/trn/trn_hcp.sql
{{ config(
    materialized='incremental',
    schema='STAGING',
    alias='TRN_HCP',
    unique_key='HCP_ID',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        h.HCP_ID,
        h.PATIENT_ID,
        h.NAME AS HCP_NAME,
        h.SPECIALITY,
        h.UPDATED_AT
    FROM {{ source('STAGING', 'STG_HCP') }} h
    INNER JOIN {{ ref('trn_patient') }} p
        ON h.PATIENT_ID = p.PATIENT_ID
)

SELECT * FROM source_data

{% if is_incremental() %}
  WHERE UPDATED_AT > (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}
