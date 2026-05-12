-- Временная таблица с метриками автомобилей
CREATE TEMPORARY TABLE tmp_vehicle_stats AS
SELECT
    v.vehicle_id AS vehicle_id,
    v.gov_number AS gov_number,
    v.model AS model,
    v.brand AS brand,
    v.fuel_type AS fuel_type,
    COUNT(f.order_key) AS trips,
    SUM(f.total_quantity) AS total_cargo_kg,
    SUM(f.total_vehicle_expense) AS total_expenses,
    AVG(f.avg_speed_kmh) AS avg_speed,
    AVG(f.fuel_expense / NULLIF(f.total_quantity, 0)) AS avg_fuel_per_kg,
    SUM(f.net_profit) AS total_profit
FROM fact_orders f
JOIN dim_vehicle v ON f.vehicle_id = v.vehicle_id
WHERE v.is_active = true
GROUP BY v.vehicle_id, v.gov_number, v.model, v.brand, v.fuel_type;

-- Анализ эффективности
SELECT
    vehicle_id,
    gov_number,
    concat(brand, ' ', model) AS car_name,
    trips,
    total_cargo_kg,
    round(total_expenses, 2) AS total_expenses,
    round(total_expenses / NULLIF(total_cargo_kg, 0), 2) AS cost_per_kg,
    round(avg_speed, 1) AS avg_speed_kmh,
    round(avg_fuel_per_kg, 4) AS avg_fuel_per_kg,
    round(total_profit, 2) AS profit,
    round(cost_per_kg / median(cost_per_kg) OVER () * 100 - 100, 1) AS cost_per_kg_deviation_pct,
    RANK() OVER (ORDER BY cost_per_kg ASC) AS efficiency_rank
FROM tmp_vehicle_stats
WHERE trips >= 5
ORDER BY efficiency_rank;