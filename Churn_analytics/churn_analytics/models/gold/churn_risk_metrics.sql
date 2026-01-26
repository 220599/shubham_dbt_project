WITH accounts AS (
    SELECT
        account_id,
        account_name,
        industry,
        country,
        plan_tier,
        signup_date,
        referral_source,
        has_churned,
        is_trial,
        seats
    FROM {{ ref('dim_account_metrics') }}
),

subscriptions AS (
    SELECT
        account_id,
        subscription_id,
        subscription_start_date,
        subscription_end_date,
        mrr_amount,
        arr_amount,
        billing_frequency,
        is_churned,
        has_upgraded,
        has_downgraded,
        auto_renew_enabled,
        subscription_length_days
    FROM {{ ref('fct_subscriptions') }}
),

-- Aggregate feature usage by account (entire historical period)
feature_engagement AS (
    SELECT
        sub.account_id,
        COUNT(DISTINCT fu.feature_name) AS distinct_features_used,
        COUNT(DISTINCT fu.usage_date) AS total_active_days,
        SUM(fu.usage_count) AS total_usage_count,
        AVG(fu.usage_count) AS avg_daily_usage,
        AVG(fu.error_rate) AS avg_error_rate,
        SUM(CASE WHEN fu.is_beta_feature = TRUE THEN 1 ELSE 0 END) AS beta_feature_usage_count,
        MIN(fu.usage_date) AS first_activity_date,
        MAX(fu.usage_date) AS last_activity_date,
        -- Engagement span: how long they actively used the product
        DATEDIFF(day, MIN(fu.usage_date), MAX(fu.usage_date)) AS engagement_span_days,
        -- Activity density: active days / engagement span
        CASE 
            WHEN DATEDIFF(day, MIN(fu.usage_date), MAX(fu.usage_date)) = 0 THEN 100.0
            ELSE CAST(COUNT(DISTINCT fu.usage_date) AS FLOAT) / 
                 DATEDIFF(day, MIN(fu.usage_date), MAX(fu.usage_date)) * 100.0
        END AS activity_density_pct
    FROM {{ ref('fct_usage') }} fu
    LEFT JOIN subscriptions sub ON fu.subscription_id = sub.subscription_id
    GROUP BY sub.account_id
),

-- Join all dimensions
combined AS (
    SELECT
        a.account_id,
        a.account_name,
        a.industry,
        a.country,
        a.plan_tier,
        a.signup_date,
        a.referral_source,
        a.has_churned,
        a.is_trial,
        a.seats,
        s.subscription_id,
        s.mrr_amount,
        s.arr_amount,
        s.billing_frequency,
        s.is_churned,
        s.has_upgraded,
        s.has_downgraded,
        s.auto_renew_enabled,
        s.subscription_length_days,
        COALESCE(fe.distinct_features_used, 0) AS distinct_features_used,
        COALESCE(fe.total_active_days, 0) AS total_active_days,
        COALESCE(fe.total_usage_count, 0) AS total_usage_count,
        COALESCE(fe.avg_daily_usage, 0) AS avg_daily_usage,
        COALESCE(fe.avg_error_rate, 0) AS avg_error_rate,
        COALESCE(fe.beta_feature_usage_count, 0) AS beta_feature_usage_count,
        fe.first_activity_date,
        fe.last_activity_date,
        COALESCE(fe.engagement_span_days, 0) AS engagement_span_days,
        COALESCE(fe.activity_density_pct, 0) AS activity_density_pct
    FROM accounts a
    LEFT JOIN subscriptions s ON a.account_id = s.account_id
    LEFT JOIN feature_engagement fe ON a.account_id = fe.account_id
),

-- Calculate churn risk score (0-100 scale) based on behavioral patterns
risk_scoring AS (
    SELECT
        c.*,
        -- ENGAGEMENT QUALITY SCORE (0-30 points)
        CASE
            WHEN c.distinct_features_used = 0 THEN 30
            WHEN c.distinct_features_used <= 2 THEN 20
            WHEN c.distinct_features_used <= 5 THEN 10
            WHEN c.distinct_features_used <= 10 THEN 5
            ELSE 0
        END +
        -- Activity consistency penalty
        CASE
            WHEN c.activity_density_pct < 20 THEN 10
            WHEN c.activity_density_pct < 40 THEN 5
            ELSE 0
        END AS engagement_quality_score,

        -- LIFECYCLE PROGRESSION SCORE (0-25 points)
        CASE
            WHEN c.subscription_length_days IS NULL THEN 0
            WHEN c.subscription_length_days < 30 THEN 20
            WHEN c.subscription_length_days < 90 THEN 15
            WHEN c.subscription_length_days < 180 THEN 10
            WHEN c.subscription_length_days < 365 THEN 5
            ELSE 0
        END +
        -- Upgrade/downgrade (now safe - columns exist)
        CASE
            WHEN c.has_upgraded = TRUE THEN -10
            WHEN c.has_downgraded = TRUE THEN 10
            ELSE 0
        END AS lifecycle_risk_score,

        -- BEHAVIOR PATTERN SCORE (0-25 points)
        CASE
            WHEN c.is_trial = TRUE AND c.subscription_length_days IS NOT NULL THEN 15
            WHEN c.is_trial = TRUE AND c.subscription_length_days IS NULL THEN 5
            ELSE 0
        END +
        CASE
            WHEN c.auto_renew_enabled = FALSE AND c.is_churned = FALSE THEN 8
            WHEN c.auto_renew_enabled = FALSE AND c.is_churned = TRUE THEN 10
            ELSE 0
        END +
        -- Error rate signal
        CASE
            WHEN c.avg_error_rate > 0.1 THEN 12
            WHEN c.avg_error_rate > 0.05 THEN 6
            ELSE 0
        END AS behavior_pattern_score,

        -- REVENUE STABILITY SCORE (0-20 points)
        CASE
            WHEN c.mrr_amount = 0 AND c.is_trial = TRUE THEN 0
            WHEN c.mrr_amount = 0 AND c.is_trial = FALSE THEN 15
            ELSE 0
        END AS revenue_stability_score,

        CURRENT_TIMESTAMP() AS dbt_created_at
    FROM combined c
)

SELECT
    account_id,
    account_name,
    industry,
    country,
    plan_tier,
    signup_date,
    referral_source,
    has_churned AS already_churned,
    is_trial,
    seats,
    subscription_id,
    mrr_amount,
    arr_amount,
    billing_frequency,
    is_churned AS subscription_churned,
    subscription_length_days,
    distinct_features_used,
    total_active_days,
    total_usage_count,
    avg_daily_usage,
    engagement_span_days,
    activity_density_pct,

    -- FINAL CHURN RISK SCORE (capped at 100)
    LEAST(
        CAST(
            engagement_quality_score +
            lifecycle_risk_score +
            behavior_pattern_score +
            revenue_stability_score
        AS INT), 100
    ) AS churn_risk_score,

    -- CHURN RISK LEVEL
    CASE
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 80 THEN 'CRITICAL'
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 60 THEN 'HIGH'
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 40 THEN 'MEDIUM'
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 20 THEN 'LOW'
        ELSE 'VERY_LOW'
    END AS churn_risk_level,

    -- REVENUE AT RISK
    CASE
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 80 THEN COALESCE(mrr_amount, 0) * 1.0
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 60 THEN COALESCE(mrr_amount, 0) * 0.75
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 40 THEN COALESCE(mrr_amount, 0) * 0.4
        ELSE COALESCE(mrr_amount, 0) * 0.1
    END AS revenue_at_risk,

    -- WHY THEY CHURNED
    CASE
        WHEN has_churned = TRUE AND distinct_features_used <= 2 THEN 'Low Feature Adoption'
        WHEN has_churned = TRUE AND subscription_length_days < 30 THEN 'Early Exit (< 30 days)'
        WHEN has_churned = TRUE AND activity_density_pct < 20 THEN 'Sporadic Usage'
        WHEN has_churned = TRUE AND has_downgraded = TRUE THEN 'Downgraded then Churned'
        WHEN has_churned = TRUE AND is_trial = TRUE THEN 'Trial Conversion Failed'
        ELSE 'Other Pattern'
    END AS churn_reason,

    -- INTERVENTION RECOMMENDATION
    CASE
        WHEN has_churned = TRUE OR is_churned = TRUE THEN 'ANALYZE_CHURN_PATTERN'
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 80 AND COALESCE(mrr_amount, 0) > 1000 THEN 'URGENT_HIGH_VALUE_RETENTION'
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 80 THEN 'URGENT_RETENTION'
        WHEN (engagement_quality_score + lifecycle_risk_score + behavior_pattern_score + revenue_stability_score) >= 60 THEN 'PROACTIVE_ENGAGEMENT'
        WHEN distinct_features_used <= 2 THEN 'FEATURE_ADOPTION_TRAINING'
        WHEN is_trial = TRUE THEN 'TRIAL_CONVERSION_SUPPORT'
        ELSE 'STANDARD_SUPPORT'
    END AS recommended_action,

    engagement_quality_score,
    lifecycle_risk_score,
    behavior_pattern_score,
    revenue_stability_score,
    dbt_created_at

FROM risk_scoring
ORDER BY churn_risk_score DESC, revenue_at_risk DESC
