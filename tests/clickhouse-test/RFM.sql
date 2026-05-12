CREATE TEMPORARY TABLE tmp_customer_rfm AS
SELECT
    c.customer_id AS customer_id,
    c.name AS name,
    c.customer_segment AS customer_segment,
    DATE_DIFF('day', MAX(t.full_date), today()) AS recency_days,
    COUNT(DISTINCT f.order_key) AS frequency,
    SUM(f.net_profit) AS monetary,
    MAX(t.full_date) AS last_order_date
FROM fact_orders f
JOIN dim_customer c ON f.customer_id = c.customer_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY c.customer_id, c.name, c.customer_segment;

-- RFM-анализ с квартилями
WITH rfm_scores AS (
    SELECT
        *,
        ntile(4) OVER (ORDER BY recency_days ASC) AS recency_score,
        ntile(4) OVER (ORDER BY frequency DESC) AS frequency_score,
        ntile(4) OVER (ORDER BY monetary DESC) AS monetary_score
    FROM tmp_customer_rfm
)
SELECT
    customer_id,
    name,
    customer_segment,
    recency_days,
    frequency,
    round(monetary, 2) AS monetary,
    last_order_date,
    recency_score,
    frequency_score,
    monetary_score,
    concat(toString(recency_score), '-', toString(frequency_score), '-', toString(monetary_score)) AS rfm_code,
    multiIf(
        recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3, 'Champions',
        recency_score >= 3 AND frequency_score >= 2, 'Loyal',
        recency_score <= 2 AND frequency_score >= 3, 'Potential',
        recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2, 'At Risk',
        'Other'
    ) AS rfm_segment
FROM rfm_scores
ORDER BY recency_score DESC, frequency_score DESC, monetary_score DESC;
--время выполнения 50 rows in set. Elapsed: 0.005 sec. --
