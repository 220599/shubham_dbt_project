

WITH source AS (
    SELECT
        usage_id,
        subscription_id,
        usage_date,
        feature_name,
        usage_count,
        usage_duration_secs,
        error_count,
        is_beta_feature
    FROM {{ ref('bronze_usage') }}
),

deduplicated AS (
    SELECT
        usage_id,
        subscription_id,
        usage_date,
        feature_name,
        usage_count,
        usage_duration_secs,
        error_count,
        is_beta_feature,
        ROW_NUMBER() OVER (
            PARTITION BY usage_id 
            ORDER BY usage_id DESC
        ) AS rn
    FROM source
    WHERE usage_id IS NOT NULL
),

cleaned AS (
    SELECT
        usage_id,
        subscription_id,
        CAST(usage_date AS DATE) AS usage_date,
        TRIM(UPPER(feature_name)) AS feature_name,
        CAST(usage_count AS INT) AS usage_count,
        CAST(usage_duration_secs AS INT) AS usage_duration_secs,
        CAST(COALESCE(error_count, 0) AS INT) AS error_count,
        CAST(is_beta_feature AS BOOLEAN) AS is_beta_feature,
        
        -- Calculate engagement metrics
        CASE
            WHEN usage_count = 0 THEN 0
            ELSE CAST(COALESCE(error_count, 0) AS FLOAT) / CAST(usage_count AS FLOAT)
        END AS error_rate,
        
        CASE
            WHEN usage_count = 0 THEN 0
            ELSE CAST(usage_duration_secs AS FLOAT) / CAST(usage_count AS FLOAT)
        END AS avg_duration_per_usage,
        
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM deduplicated
    WHERE rn = 1
)

SELECT * FROM cleaned
