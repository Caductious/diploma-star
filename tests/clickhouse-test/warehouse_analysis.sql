-- Временная таблица с недельными итогами по складам
CREATE TEMPORARY TABLE tmp_weekly_warehouse AS
SELECT
    w.warehouse_id AS warehouse_id,
    w.name AS warehouse_name,
    toStartOfWeek(t.full_date) AS week_start,
    SUM(f.net_profit) AS weekly_profit,
    SUM(f.order_amount) AS weekly_revenue
FROM fact_orders f
JOIN dim_warehouse w ON f.warehouse_id = w.warehouse_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE t.full_date >= '2023-01-01'
GROUP BY w.warehouse_id, w.name, week_start;

-- Кумулятивный анализ с ростом
SELECT
    warehouse_name,
    week_start,
    round(weekly_profit, 2) AS weekly_profit,
    round(SUM(weekly_profit) OVER (PARTITION BY warehouse_id ORDER BY week_start), 2) AS cum_profit,
    round(weekly_revenue, 2) AS weekly_revenue,
    round(weekly_profit / NULLIF(lag(weekly_profit) OVER (PARTITION BY warehouse_id ORDER BY week_start), 0) * 100 - 100, 1) AS wow_growth_pct,
    round(AVG(weekly_profit) OVER (PARTITION BY warehouse_id ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ma4_profit
FROM tmp_weekly_warehouse
ORDER BY warehouse_name, week_start DESC

--0.004 sec--