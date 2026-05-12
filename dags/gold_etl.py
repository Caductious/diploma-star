from calendar import day_name
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from clickhouse_driver import Client
import pandas as pd


load_dotenv()

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"

default_args = {
    'owner': 'Loginov Petr',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def get_clickhouse_client():
    """Создание и возврат клиента для подключения к ClickHouse"""
    print(f"DEBUG: HOST = {os.getenv('CLICKHOUSEHOST')}")
    print(f"DEBUG: PORT = {os.getenv('CLICKHOUSEPORT')}")
    print(f"DEBUG: USER = {os.getenv('CLICKHOUSEUSER')}")
    return Client(
        host=os.getenv('CLICKHOUSEHOST'),
        port=int(os.getenv('CLICKHOUSEPORT')),
        user=os.getenv('CLICKHOUSEUSER'),
        password=os.getenv('CLICKHOUSEPASSWORD')
        )     


def load_dim_time(**context):
    """
    Загрузка измерений времени (календарь) в таблицу dim_time:
    - Генерация непрерывного диапазона дат с 2024-01-01 по 2029-12-31
    - Для каждой даты вычисляются атрибуты: год, квартал, месяц, день недели,
      выходной/рабочий день, номер недели, финансовый год
    - Формирование time_id в формате YYYYMMDD (числовой)
    - Очистка существующей таблицы и загрузка новых данных
    """
    client = get_clickhouse_client()
    dates = []
    start_date = datetime(2024, 1, 1).date()
    end_date = datetime(2029, 12, 31).date()

    current = start_date
    while current <= end_date:
        time_id = int(current.strftime('%Y%m%d'))
        dates.append({
            'time_id': time_id,
            'full_date': current,
            'year': current.year,
            'quarter': (current.month - 1) // 3 + 1,
            'month': current.month,
            'day_of_week': current.weekday() + 1,
            'day': current.day,
            'day_of_week_name': day_name[current.weekday()],
            'is_weekend': current.weekday() >= 5,
            'is_holiday': False,
            'holiday_name': '',
            'week_num': current.isocalendar()[1],
            'fiscal_year': current.year
        })
        current += timedelta(days=1)
    
    client.execute("TRUNCATE TABLE gold.dim_time")
    client.execute(
        "INSERT INTO gold.dim_time (time_id, full_date, year, quarter, month, day_of_week, day, "
        "day_of_week_name, is_weekend, is_holiday, holiday_name, week_num, fiscal_year) VALUES",
        dates
        )
    
    print(f"Загружено {len(dates)} дат в dim_time")
    return len(dates)


def load_dim_customers(**context):
    """
    Загрузка измерений клиентов в таблицу dim_customer:
    - Чтение очищенных данных из silver/postgres/customers.parquet
    - Преобразование типов данных для соответствия схеме ClickHouse
    - Очистка существующей таблицы и загрузка новых данных
    """
    client = get_clickhouse_client()
    df = pd.read_parquet(f"{SILVER_PATH}/postgres/customers.parquet")
    
    data = []
    for i, row in df.iterrows():
        data.append({
            'customer_id': int(row['id']),
            'name': str(row['name']),
            'short_name': str(row.get('short_name', '')),
            'inn': str(row.get('inn', '')),
            'ognr': str(row.get('ognr', '')),
            'contact_person': str(row.get('contact_person', '')),
            'phone': str(row.get('phone', '')),
            'email': str(row.get('email', '')),
            'address': str(row.get('address', '')),
            'customer_segment': str(row.get('customer_segment', '')),
            'credit_limit': float(row.get('credit_limit', 0)),
            'is_active': bool(row.get('is_active', True)),
            'registration_date': row.get('registration_date', datetime.now().date()),
            'etl_updated_at': datetime.now()
        })
    
    client.execute("TRUNCATE TABLE gold.dim_customer")
    client.execute(
        "INSERT INTO gold.dim_customer (customer_id, name, short_name, inn, ognr, contact_person, "
        "phone, email, address, customer_segment, credit_limit, is_active, registration_date, etl_updated_at) VALUES",
        data
    )
    
    print(f"Loaded {len(data)} customers into dim_customer")
    return len(data)


def load_dim_drivers(**context):
    """
    Загрузка измерений водителей в таблицу dim_driver:
    - Чтение очищенных данных из silver/postgres/drivers.parquet
    - Разделение полного имени ФИО на составные части (фамилия, имя, отчество)
    - Преобразование типов данных
    - Очистка существующей таблицы и загрузка новых данных
    """
    client = get_clickhouse_client()
    
    df = pd.read_parquet(f"{SILVER_PATH}/postgres/drivers.parquet")
    
    data = []
    for _, row in df.iterrows():
        full_name = str(row.get('full_name', ''))
        name_parts = full_name.split()
        
        data.append({
            'driver_id': int(row['id']),
            'full_name': full_name,
            'last_name': name_parts[0] if len(name_parts) > 0 else '',
            'first_name': name_parts[1] if len(name_parts) > 1 else '',
            'middle_name': name_parts[2] if len(name_parts) > 2 else '',
            'license_number': str(row.get('license_number', '')),
            'phone': str(row.get('phone', '')),
            'email': str(row.get('email', '')),
            'is_active': bool(row.get('is_active', True)),
            'etl_updated_at': datetime.now()
        })
    
    client.execute("TRUNCATE TABLE gold.dim_driver")
    client.execute(
        "INSERT INTO gold.dim_driver (driver_id, full_name, last_name, first_name, middle_name, "
        "license_number, phone, email, is_active, etl_updated_at) VALUES",
        data
    )
    
    print(f"Загружено {len(data)} водителей в dim_driver")
    return len(data)


def load_dim_vehicles(**context):
    """
    Загрузка измерений транспортных средств в таблицу dim_vehicle:
    - Чтение очищенных данных из silver/postgres/vehicles.parquet
    - Преобразование типов данных
    - Установка значений по умолчанию для отсутствующих полей
    - Очистка существующей таблицы и загрузка новых данных
    """
    client = get_clickhouse_client()
    
    df = pd.read_parquet(f"{SILVER_PATH}/postgres/vehicles.parquet")
    
    data = []
    for _, row in df.iterrows():
        data.append({
            'vehicle_id': int(row['id']),
            'gov_number': str(row.get('plate_number', '')),
            'model': str(row.get('model', '')),
            'brand': str(row.get('brand', '')),
            'fuel_type': str(row.get('fuel_type', 'gasoline')),
            'engine_capacity': float(row.get('engine_capacity', 1.6)),
            'max_load_kg': int(row.get('max_load_kg', 0)),
            'trailer_number': str(row.get('trailer_number', '')),
            'is_active': bool(row.get('is_active', True)),
            'etl_updated_at': datetime.now()
        })
    
    client.execute("TRUNCATE TABLE gold.dim_vehicle")
    client.execute(
        "INSERT INTO gold.dim_vehicle (vehicle_id, gov_number, model, brand, "
        "fuel_type, engine_capacity, max_load_kg, trailer_number, is_active, "
        "etl_updated_at) VALUES",
        data
    )
    
    print(f"Загружено {len(data)} ТС в dim_vehicle")
    return len(data)


def load_dim_warehouses(**context):
    """
    Загрузка измерений складов в таблицу dim_warehouse:
    - Чтение очищенных данных из silver/postgres/warehouses.parquet
    - Преобразование типов данных
    - Установка значений по умолчанию для отсутствующих полей
    - Очистка существующей таблицы и загрузка новых данных
    """
    client = get_clickhouse_client()
    
    df = pd.read_parquet(f"{SILVER_PATH}/postgres/warehouses.parquet")
    
    data = []
    for _, row in df.iterrows():
        data.append({
            'warehouse_id': int(row['id']),
            'name': str(row.get('name', '')),
            'code': str(row.get('code', '')),
            'address': str(row.get('address', '')),
            'city': str(row.get('city', '')),
            'region': str(row.get('region', '')),
            'responsible_person': str(row.get('responsible_person', '')),
            'is_active': bool(row.get('is_active', True)),
            'warehouse_type': str(row.get('warehouse_type', 'standard')),
            'etl_updated_at': datetime.now()
        })
    
    client.execute("TRUNCATE TABLE gold.dim_warehouse")
    client.execute(
        "INSERT INTO gold.dim_warehouse (warehouse_id, name, code, address, city, region, "
        "responsible_person, is_active, warehouse_type, etl_updated_at) VALUES",
        data
    )
    
    print(f"Загружено {len(data)} складов в dim_warehouse")
    return len(data)


def load_dim_products(**context):
    """
    Загрузка измерений товаров в таблицу dim_product:
    - Чтение очищенных данных из silver/postgres/products.parquet
    - Преобразование типов данных
    - Установка цен по умолчанию (0 для отсутствующих)
    - Очистка существующей таблицы и загрузка новых данных
    """
    client = get_clickhouse_client()
    
    df = pd.read_parquet(f"{SILVER_PATH}/postgres/products.parquet")
    
    data = []
    for _, row in df.iterrows():
        data.append({
            'product_id': int(row['id']),
            'sku': str(row.get('sku', '')),
            'name': str(row.get('name', '')),
            'category': str(row.get('category', '')),
            'unit': str(row.get('unit', 'pcs')),
            'cost_price': float(row.get('cost_price', 0)),
            'selling_price': float(row.get('selling_price', 0)),
            'is_active': bool(row.get('is_active', True)),
            'etl_updated_at': datetime.now()
        })
    
    client.execute("TRUNCATE TABLE gold.dim_product")
    client.execute(
        "INSERT INTO gold.dim_product (product_id, sku, name, category, unit, cost_price, selling_price, is_active, etl_updated_at) VALUES",
        data
    )
    
    print(f"Загружено {len(data)} продуктов into dim_product")
    return len(data)


def aggregate_expenses(**context):
    """
    Агрегация расходов по транспортным средствам и датам:
    - Чтение очищенных данных о расходах из silver/excel/all_expenses.parquet
    - Создание колонок с расходами по категориям (fuel, other, wash)
    - Группировка по vehicle_id и expense_date с суммированием расходов по каждой категории
    - Округление сумм до 2 знаков после запятой
    - Сохранение агрегированного результата в silver/aggregated/expenses_by_vehicle.parquet
    """
    file_path = f"{SILVER_PATH}/excel/all_expenses.parquet"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        expenses_agg = pd.DataFrame(columns=['vehicle_id', 'expense_date', 
                                              'fuel_expense', 'other_expense', 'wash_expense', 'total_expense'])
        os.makedirs(f"{SILVER_PATH}/aggregated", exist_ok=True)
        expenses_agg.to_parquet(f"{SILVER_PATH}/aggregated/expenses_by_vehicle.parquet", index=False)
        return 0
    
    df = pd.read_parquet(file_path)
    
    df['fuel_expense'] = df.apply(
        lambda row: row['amount_rub'] if row['expense_category_en'] == 'fuel' else 0, 
        axis=1
    )
    
    df['other_expense'] = df.apply(
        lambda row: row['amount_rub'] if row['expense_category_en'] == 'other' else 0, 
        axis=1
    )
    
    df['wash_expense'] = df.apply(
        lambda row: row['amount_rub'] if row['expense_category_en'] == 'wash' else 0, 
        axis=1
    )
    
    expenses_agg = df.groupby(['vehicle_id', 'expense_date']).agg(
        fuel_expense=('fuel_expense', 'sum'),
        other_expense=('other_expense', 'sum'),
        wash_expense=('wash_expense', 'sum'),
        total_expense=('amount_rub', 'sum')
    ).reset_index()
    
    for col in ['fuel_expense', 'other_expense', 'wash_expense', 'total_expense']:
        expenses_agg[col] = expenses_agg[col].round(2)
    
    os.makedirs(f"{SILVER_PATH}/aggregated", exist_ok=True)
    expenses_agg.to_parquet(f"{SILVER_PATH}/aggregated/expenses_by_vehicle.parquet", index=False)
    
    print(f"Затраты агрегированы. Итого: {len(expenses_agg)} записей")    
    return len(expenses_agg)


def build_fact_orders(**context):
    """
    Построение таблицы фактов заказов (fact_orders):
    - Загрузка продаж из silver/postgres/sale_headers.parquet
    - Маппинг госномеров транспортных средств на vehicle_id
    - Загрузка агрегированных расходов по автомобилям
    - Загрузка и обработка телеметрии для расчета длительности поездок и средней скорости
    - Группировка телеметрии по order_key для получения метрик поездки
    - Формирование фактов: для каждого заказа вычисляются расходы, метрики поездки,
      валовая и чистая прибыль, процент маржинальности
    - Пакетная вставка в ClickHouse (пакетами по 1000 записей)
    - Очистка существующей таблицы перед загрузкой
    """
    client = get_clickhouse_client()
    
    sales = pd.read_parquet(f"{SILVER_PATH}/postgres/sale_headers.parquet")
    sales['created_at'] = pd.to_datetime(sales['created_at'])
    sales['sale_date'] = sales['created_at'].dt.date
    
    vehicles_df = pd.read_parquet(f"{SILVER_PATH}/postgres/vehicles.parquet")
    plate_to_vehicle_id = dict(zip(
        vehicles_df['plate_number'].astype(str).str.upper().str.strip(),
        vehicles_df['id']
    ))
    
    expenses_path = f"{SILVER_PATH}/aggregated/expenses_by_vehicle.parquet"
    if os.path.exists(expenses_path):
        expenses = pd.read_parquet(expenses_path)
        expenses['expense_date'] = pd.to_datetime(expenses['expense_date']).dt.date
        expenses_dict = {}
        for _, row in expenses.iterrows():
            key = (row['vehicle_id'], row['expense_date'])
            expenses_dict[key] = {
                'fuel': float(row['fuel_expense']) if pd.notna(row['fuel_expense']) else 0.0,
                'other': float(row['other_expense']) if pd.notna(row['other_expense']) else 0.0,
                'wash': float(row['wash_expense']) if pd.notna(row['wash_expense']) else 0.0,
                'total': float(row['total_expense']) if pd.notna(row['total_expense']) else 0.0
            }
    else:
        expenses_dict = {}
    
    telemetry_path = f"{SILVER_PATH}/json/telemetry.parquet"
    trip_metrics = {}
    
    if os.path.exists(telemetry_path):
        print("Загрузка данных телеметрии...")
        telemetry = pd.read_parquet(telemetry_path)
        
        telemetry['timestamp'] = pd.to_datetime(telemetry['timestamp'])
        telemetry['event_date'] = telemetry['timestamp'].dt.date
        
        if 'order_key' in telemetry.columns:
            telemetry = telemetry.dropna(subset=['order_key'])
            telemetry['order_key'] = telemetry['order_key'].astype(int)
            
            if not telemetry.empty:
                print(f"Обработка телеметрии для {telemetry['order_key'].nunique()} уникальных заказов")
                
                for order_key, group in telemetry.groupby('order_key'):
                    if len(group) < 2:
                        continue
                    
                    group = group.sort_values('timestamp')
                    
                    first_time = group['timestamp'].iloc[0]
                    last_time = group['timestamp'].iloc[-1]
                    duration_minutes = int((last_time - first_time).total_seconds() / 60.0)
                    print(f"поездка: {first_time} - {last_time}")
                    
                    if 'spd' in group.columns:
                        avg_speed = float(group['spd'].mean())
                    else:
                        avg_speed = 0.0
                    
                    trip_metrics[order_key] = {
                        'duration_minutes': duration_minutes,
                        'avg_speed_kmh': round(avg_speed, 2)
                    }
                
                print(f"Посчитаны метрики для {len(trip_metrics)} заказов из логов телеметрии")
        else:
            print("ВНИМАНИЕ: в телеметрии отсутствует колонка order_key, метрики поездок не будут рассчитаны")
    
    facts = []
    matched_trips = 0
    
    for _, sale in sales.iterrows():
        sale_date = sale['sale_date']
        time_id = int(sale_date.strftime('%Y%m%d'))
        vehicle_id = int(sale.get('vehicle_id', 0))

        fuel_expense = 0.0
        other_expense = 0.0
        wash_expense = 0.0
        total_expense = 0.0
        
        expense_key = (vehicle_id, sale_date)
        if expense_key in expenses_dict:
            fuel_expense = float(expenses_dict[expense_key]['fuel'])
            other_expense = float(expenses_dict[expense_key]['other'])
            wash_expense = float(expenses_dict[expense_key]['wash'])
            total_expense = float(expenses_dict[expense_key]['total'])
        
        order_key_value = int(sale['id'])
        if order_key_value in trip_metrics:
            trip_duration = int(trip_metrics[order_key_value]['duration_minutes'])
            avg_speed = float(trip_metrics[order_key_value]['avg_speed_kmh'])
            matched_trips += 1
        else:
            trip_duration = 0
            avg_speed = 0.0
        
        order_amount = float(sale['total_amount'])
        gross_profit = order_amount
        net_profit = gross_profit - total_expense
        margin_percent = (net_profit / order_amount * 100) if order_amount > 0 else 0.0
        
        fact = {
            'order_key': order_key_value,
            'time_id': int(time_id),
            'customer_id': int(sale.get('customer_id', 0)),
            'driver_id': int(sale.get('driver_id', 0)),
            'vehicle_id': int(vehicle_id),
            'warehouse_id': int(sale.get('warehouse_id', 0)),
            'order_amount': float(order_amount),
            'total_quantity': 1,
            'gross_profit': float(gross_profit),
            'fuel_expense': float(fuel_expense),
            'other_expense': float(other_expense),
            'wash_expense': float(wash_expense),
            'total_vehicle_expense': float(total_expense),
            'trip_duration_minutes': int(trip_duration),
            'avg_speed_kmh': float(avg_speed),
            'net_profit': float(net_profit),
            'margin_percent': float(margin_percent),
            'etl_loaded_at': datetime.now()
        }
        facts.append(fact)
        
    print(f"Сопоставлено поездок с телеметрией: {matched_trips} из {len(facts)} заказов")
        
    if facts:
        client.execute("TRUNCATE TABLE gold.fact_orders")
        
        batch_size = 1000
        for i in range(0, len(facts), batch_size):
            batch = facts[i:i+batch_size]
            client.execute(
                "INSERT INTO gold.fact_orders (order_key, time_id, customer_id, driver_id, vehicle_id, "
                "warehouse_id, order_amount, total_quantity, gross_profit, fuel_expense, other_expense, "
                "wash_expense, total_vehicle_expense, trip_duration_minutes, avg_speed_kmh, net_profit, "
                "margin_percent, etl_loaded_at) VALUES",
                batch
            )
            print(f"Inserted batch {i//batch_size + 1}/{(len(facts)-1)//batch_size + 1}")
    else:
        print("ERROR: Нет данных для вставки в таблицу фактов")

    print(f"Создано {len(facts)} записей в таблице фактов")
    return len(facts)



with DAG(
    'gold_layer_clickhouse',
    default_args=default_args,
    description='Load silver to ClickHouse and build star schema',
    schedule_interval='@daily',
    catchup=False,
    tags=['diploma', 'gold', 'clickhouse'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    load_time = PythonOperator(
        task_id='load_dim_time',
        python_callable=load_dim_time,
    )
    
    load_customers = PythonOperator(
        task_id='load_dim_customers',
        python_callable=load_dim_customers,
    )
    
    load_drivers = PythonOperator(
        task_id='load_dim_drivers',
        python_callable=load_dim_drivers,
    )
    
    load_vehicles = PythonOperator(
        task_id='load_dim_vehicles',
        python_callable=load_dim_vehicles,
    )
    
    load_warehouses = PythonOperator(
        task_id='load_dim_warehouses',
        python_callable=load_dim_warehouses,
    )
    
    load_products = PythonOperator(
        task_id='load_dim_products',
        python_callable=load_dim_products,
    )
    
    aggregate_exp = PythonOperator(
        task_id='aggregate_expenses',
        python_callable=aggregate_expenses,
    )
    
    build_facts = PythonOperator(
        task_id='build_fact_orders',
        python_callable=build_fact_orders,
    )
    
    start >> load_time
    load_time >> [load_customers, load_drivers, load_vehicles, load_warehouses, load_products]
    load_time >> aggregate_exp
    [load_customers, load_drivers, load_vehicles, load_warehouses, load_products, aggregate_exp] >> build_facts
    build_facts >> end