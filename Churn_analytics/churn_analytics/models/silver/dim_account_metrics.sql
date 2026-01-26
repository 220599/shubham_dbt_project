{{ config(
    materialized='table',
    schema='silver',
    indexes=[
        {'columns': ['account_id'], 'unique': True}
    ]
) }}

WITH source AS (
    SELECT
        account_id,
        account_name,
        industry,
        country,
        signup_date,
        referral_source,
        plan_tier,
        seats,
        is_trial,
        churn_flag
    FROM {{ ref('bronze_accounts') }}
),

deduplicated AS (
    SELECT
        account_id,
        account_name,
        industry,
        country,
        signup_date,
        referral_source,
        plan_tier,
        seats,
        is_trial,
        churn_flag,
        ROW_NUMBER() OVER (
            PARTITION BY account_id 
            ORDER BY account_id DESC
        ) AS rn
    FROM source
    WHERE account_id IS NOT NULL
),

cleaned AS (
    SELECT
        account_id,
        TRIM(account_name) AS account_name,
        UPPER(TRIM(COALESCE(industry, 'Unknown'))) AS industry,
        UPPER(TRIM(COALESCE(country, 'Unknown'))) AS country,
        CAST(signup_date AS DATE) AS signup_date,
        UPPER(TRIM(COALESCE(referral_source, 'Unknown'))) AS referral_source,
        UPPER(TRIM(COALESCE(plan_tier, 'Unknown'))) AS plan_tier,
        CAST(seats AS INT) AS seats,
        CAST(is_trial AS BOOLEAN) AS is_trial,
        CAST(churn_flag AS BOOLEAN) AS has_churned,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM deduplicated
    WHERE rn = 1
)

SELECT * FROM cleaned
