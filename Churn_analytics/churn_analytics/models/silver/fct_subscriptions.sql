
WITH source AS (
    SELECT
        subscription_id,
        account_id,
        start_date,
        end_date,
        plan_tier,
        seats,
        mrr_amount,
        arr_amount,
        is_trial,
        upgrade_flag,
        downgrade_flag,
        churn_flag,
        billing_frequency,
        auto_renew_flag
    FROM {{ ref('bronze_subscriptions') }}
),

deduplicated AS (
    SELECT
        subscription_id,
        account_id,
        start_date,
        end_date,
        plan_tier,
        seats,
        mrr_amount,
        arr_amount,
        is_trial,
        upgrade_flag,
        downgrade_flag,
        churn_flag,
        billing_frequency,
        auto_renew_flag,
        ROW_NUMBER() OVER (
            PARTITION BY subscription_id 
            ORDER BY subscription_id DESC
        ) AS rn
    FROM source
    WHERE subscription_id IS NOT NULL
),

cleaned AS (
    SELECT
        subscription_id,
        account_id,
        CAST(start_date AS DATE) AS subscription_start_date,
        -- NULL end_date means active subscription
        CAST(end_date AS DATE) AS subscription_end_date,
        UPPER(TRIM(COALESCE(plan_tier, 'UNKNOWN'))) AS plan_tier,
        CAST(seats AS INT) AS seats,
        CAST(COALESCE(mrr_amount, 0) AS DECIMAL(12,2)) AS mrr_amount,
        CAST(COALESCE(arr_amount, 0) AS DECIMAL(12,2)) AS arr_amount,
        CAST(is_trial AS BOOLEAN) AS is_trial,
        CAST(upgrade_flag AS BOOLEAN) AS has_upgraded,
        CAST(downgrade_flag AS BOOLEAN) AS has_downgraded,
        CAST(churn_flag AS BOOLEAN) AS is_churned,
        UPPER(TRIM(COALESCE(billing_frequency, 'MONTHLY'))) AS billing_frequency,
        CAST(auto_renew_flag AS BOOLEAN) AS auto_renew_enabled,
        
        -- Calculate subscription length (only if ended)
        CASE 
            WHEN end_date IS NOT NULL 
            THEN DATEDIFF(day, CAST(start_date AS DATE), CAST(end_date AS DATE))
            ELSE NULL
        END AS subscription_length_days,
        
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM deduplicated
    WHERE rn = 1
)

SELECT * FROM cleaned
