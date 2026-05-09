-- Создание базы данных
CREATE DATABASE IF NOT EXISTS gold;

USE gold;

-- Измерение: dim_time
CREATE TABLE IF NOT EXISTS dim_time (
    time_id UInt32,
    full_date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    day_of_week UInt8,
    day UInt8,
    day_of_week_name String,
    is_weekend Bool,
    is_holiday Bool,
    holiday_name String,
    week_num UInt8,
    fiscal_year UInt16
) ENGINE = MergeTree()
ORDER BY time_id;

-- Измерение: dim_customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id UInt64,
    name String,
    short_name String,
    inn String,
    ognr String,
    contact_person String,
    phone String,
    email String,
    address String,
    customer_segment String,
    credit_limit Decimal(15,2),
    is_active Bool,
    registration_date Date,
    etl_updated_at DateTime
) ENGINE = ReplacingMergeTree(etl_updated_at)
ORDER BY customer_id;

-- Измерение: dim_driver
CREATE TABLE IF NOT EXISTS dim_driver (
    driver_id UInt64,
    full_name String,
    last_name String,
    first_name String,
    middle_name String,
    license_number String,
    license_category String,
    phone String,
    email String,
    is_active Bool,
    etl_updated_at DateTime
) ENGINE = ReplacingMergeTree(etl_updated_at)
ORDER BY driver_id;

-- Измерение: dim_vehicle
CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id UInt64,
    gov_number String,
    model String,
    brand String,
    fuel_type String,
    engine_capacity Decimal(5,1),
    max_load_kg UInt32,
    trailer_number String,
    is_active Bool,
    etl_updated_at DateTime
) ENGINE = ReplacingMergeTree(etl_updated_at)
ORDER BY vehicle_id;

-- Измерение: dim_warehouse
CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_id UInt64,
    name String,
    code String,
    address String,
    city String,
    region String,
    responsible_person String,
    is_active Bool,
    warehouse_type String,
    etl_updated_at DateTime
) ENGINE = ReplacingMergeTree(etl_updated_at)
ORDER BY warehouse_id;

CREATE TABLE IF NOT EXISTS dim_product (
    product_id UInt64,
    sku String,
    name String,
    category String,
    unit String,
    cost_price Decimal(15,2),
    selling_price Decimal(15,2),
    is_active Bool,
    etl_updated_at DateTime
) ENGINE = ReplacingMergeTree(etl_updated_at)
ORDER BY product_id;

-- Таблица фактов: fact_orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key UInt64,
    sale_id String,
    time_id UInt32,
    customer_id UInt64,
    driver_id UInt64,
    vehicle_id UInt64,
    warehouse_id UInt64,
    order_amount Decimal(15,2),
    total_quantity UInt32,
    gross_profit Decimal(15,2),
    fuel_expense Decimal(15,2),
    other_expense Decimal(15,2),
    wash_expense Decimal(15,2),
    total_vehicle_expense Decimal(15,2),
    trip_duration_minutes UInt32,
    avg_speed_kmh Float32,
    net_profit Decimal(15,2),
    margin_percent Float32,
    etl_loaded_at DateTime
) ENGINE = MergeTree()
ORDER BY (time_id, order_key);

