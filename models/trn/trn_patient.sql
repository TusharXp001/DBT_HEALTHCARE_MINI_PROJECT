-- models/trn/trn_patient.sql
{{ config(
    materialized='incremental',
    schema='STAGING',
    alias='TRN_PATIENT',
    unique_key='PATIENT_ID',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        PATIENT_ID,
        UPPER(FIRST_NAME) AS FIRST_NAME,
        UPPER(LAST_NAME)  AS LAST_NAME,
        GENDER,
        DATE_OF_BIRTH,
        UPDATED_AT
    FROM {{ source('STAGING', 'STG_PATIENT') }}
)

SELECT * FROM source_data

{% if is_incremental() %}
  WHERE UPDATED_AT > (SELECT MAX(UPDATED_AT) FROM {{ this }})
{% endif %}
