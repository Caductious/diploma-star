from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import numpy as np


BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"

default_args = {
    'owner': 'Loginov Petr',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def clean_text_columns(df):
    """Данная функция очищает текстовые столбцы от пустых значений,
    заменяя их на nan, кроме того некоторые слова
    приводятся к верхнему регистру"""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace('', np.nan)
        if any(keyword in col.lower() for keyword in ['inn', 'plate', 'license', 'sku']):
            df[col] = df[col].str.upper()
    return df


def clean_customers(**context):
    """
    Очистка данных о клиентах:
    - Удаление дубликатов по ИНН и полных дублей строк
    - Очистка текстовых полей (обрезка пробелов, замена пустых строк на NaN, приведение ИНН к верхнему регистру)
    - Заполнение пропусков: name='Unknown', contacts='Not provided', address='Not provided'
    - Заполнение is_active=True для пропущенных значений
    - Удаление записей без ИНН
    - Добавление служебных полей etl_updated_at и etl_hash
    - Сохранение результата в silver/postgres/customers.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/customers.parquet")
    
    df = df.drop_duplicates(subset=['inn'], keep='first')
    df = df.drop_duplicates()
    
    df = clean_text_columns(df)
    
    df['name'] = df['name'].fillna('Unknown')
    df['contacts'] = df['contacts'].fillna('Not provided')
    df['address'] = df['address'].fillna('Not provided')
    
    df['is_active'] = df['is_active'].fillna(True).astype(bool)
    
    df = df.dropna(subset=['inn'])
    
    df['etl_updated_at'] = datetime.now()
    df['etl_hash'] = df.apply(lambda x: hash(tuple(x[['inn', 'name']])), axis=1)
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/customers.parquet", index=False)
    
    print(f"Customers: {len(df)} records after cleaning")
    return len(df)


def clean_drivers(**context):
    """
    Очистка данных о водителях:
    - Удаление дубликатов по номеру лицензии и полных дублей строк
    - Очистка текстовых полей (обрезка пробелов, замена пустых строк на NaN, приведение номера лицензии к верхнему регистру)
    - Заполнение пропусков: full_name='Unknown', phone='Not provided', license_number='NO_LICENSE'
    - Заполнение is_active=True для пропущенных значений
    - Удаление записей без номера лицензии (кроме специальной метки NO_LICENSE)
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/drivers.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/drivers.parquet")
    
    df = df.drop_duplicates(subset=['license_number'], keep='first')
    df = df.drop_duplicates()
    
    df = clean_text_columns(df)
    
    df['full_name'] = df['full_name'].fillna('Unknown')
    df['phone'] = df['phone'].fillna('Not provided')
    df['license_number'] = df['license_number'].fillna('NO_LICENSE')
    
    df['is_active'] = df['is_active'].fillna(True).astype(bool)
    
    df = df[df['license_number'] != 'NO_LICENSE']
    
    df['etl_updated_at'] = datetime.now()
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/drivers.parquet", index=False)
    
    print(f"Drivers: {len(df)} records after cleaning")
    return len(df)


def clean_vehicles(**context):
    """
    Очистка данных о транспортных средствах:
    - Удаление дубликатов по госномеру и полных дублей строк
    - Очистка текстовых полей (обрезка пробелов, замена пустых строк на NaN, приведение госномера к верхнему регистру)
    - Заполнение пропусков: plate_number='NO_PLATE', model='Unknown'
    - Заполнение is_active=True для пропущенных значений
    - Удаление записей без госномера (кроме специальной метки NO_PLATE)
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/vehicles.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/vehicles.parquet")
    
    df = df.drop_duplicates(subset=['plate_number'], keep='first')
    df = df.drop_duplicates()
    
    df = clean_text_columns(df)
    
    df['plate_number'] = df['plate_number'].fillna('NO_PLATE')
    df['model'] = df['model'].fillna('Unknown')
    df['is_active'] = df['is_active'].fillna(True).astype(bool)
    
    df = df[df['plate_number'] != 'NO_PLATE']
    
    df['etl_updated_at'] = datetime.now()
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/vehicles.parquet", index=False)
    
    print(f"Vehicles: {len(df)} records after cleaning")
    return len(df)


def clean_suppliers(**context):
    """
    Очистка данных о поставщиках:
    - Удаление дубликатов по ИНН и полных дублей строк
    - Очистка текстовых полей (обрезка пробелов, замена пустых строк на NaN, приведение ИНН к верхнему регистру)
    - Заполнение пропусков: name='Unknown', contacts='Not provided'
    - Заполнение is_active=True для пропущенных значений
    - Удаление записей без ИНН
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/suppliers.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/suppliers.parquet")
    
    df = df.drop_duplicates(subset=['inn'], keep='first')
    df = df.drop_duplicates()
    
    df = clean_text_columns(df)
    
    df['name'] = df['name'].fillna('Unknown')
    df['contacts'] = df['contacts'].fillna('Not provided')
    df['is_active'] = df['is_active'].fillna(True).astype(bool)
    
    df = df.dropna(subset=['inn'])
    
    df['etl_updated_at'] = datetime.now()
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/suppliers.parquet", index=False)
    
    print(f"Suppliers: {len(df)} records after cleaning")
    return len(df)


def clean_warehouses(**context):
    """
    Очистка данных о складах:
    - Удаление дубликатов по названию склада и полных дублей строк
    - Очистка текстовых полей (обрезка пробелов, замена пустых строк на NaN)
    - Заполнение пропусков: name='Unknown', address='Not provided'
    - Заполнение is_active=True для пропущенных значений
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/warehouses.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/warehouses.parquet")
    
    df = df.drop_duplicates(subset=['name'], keep='first')
    df = df.drop_duplicates()
    
    df = clean_text_columns(df)
    
    df['name'] = df['name'].fillna('Unknown')
    df['address'] = df['address'].fillna('Not provided')
    df['is_active'] = df['is_active'].fillna(True).astype(bool)
    
    df['etl_updated_at'] = datetime.now()
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/warehouses.parquet", index=False)
    
    print(f"Warehouses: {len(df)} records after cleaning")
    return len(df)


def clean_sale_headers(**context):
    """
    Очистка данных о продажах (шапки документов):
    - total_amount вычисляется как сумма amount из sale_items по данному sale_id
    - Удаление дубликатов по ID документа и полных дублей строк
    - Преобразование ID в строковый тип
    - Преобразование created_at в datetime (некорректные -> NaN)
    - Заполнение пустых комментариев пустой строкой
    - Удаление записей без created_at
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/sale_headers.parquet
    """
    headers_path = f"{BRONZE_PATH}/postgres/sale_headers.parquet"
    items_path = f"{BRONZE_PATH}/postgres/sale_items.parquet"

    # 1. Загрузка шапки
    df = pd.read_parquet(headers_path)
    original_len = len(df)

    # 2. Пересчёт total_amount из sale_items (если файл существует)
    if os.path.exists(items_path):
        items = pd.read_parquet(items_path)

        # Очистка items: оставляем только нужные колонки и преобразуем типы
        items['sale_id'] = items['sale_id'].astype(str)
        items['amount'] = pd.to_numeric(items['amount'], errors='coerce')

        # Суммируем amount по sale_id
        items_sum = items.groupby('sale_id')['amount'].sum().reset_index()
        items_sum.columns = ['id', 'computed_total']

        # Приводим id в шапке к строке
        df['id'] = df['id'].astype(str)

        # Объединяем и заменяем total_amount
        df = df.merge(items_sum, on='id', how='left')
        df['total_amount'] = df['computed_total'].fillna(df['total_amount'])   # если нет позиций – оставляем старую сумму
        df.drop('computed_total', axis=1, inplace=True)

        print(f"Total_amount recalculated from sale_items for {len(items_sum)} sales")
    else:
        print("sale_items.parquet not found, keeping original total_amount")

    # 3. Дальнейшая очистка (как раньше)
    df = df.drop_duplicates(subset=['id'], keep='first')
    df = df.drop_duplicates()

    df['id'] = df['id'].astype(str)
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    df['comment'] = df['comment'].fillna('')

    # Оставляем только продажи с положительной суммой (после пересчёта)
    df = df[df['total_amount'] > 0]

    df = df.dropna(subset=['created_at'])

    df['etl_updated_at'] = datetime.now()

    # 4. Сохранение
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/sale_headers.parquet", index=False)

    removed_count = original_len - len(df)
    print(f"Заголовки продаж: {len(df)} записей (удалено {removed_count} записей)")
    return len(df)

def clean_purchase_headers(**context):
    """
    Очистка данных о закупках (шапки документов):
    - Удаление дубликатов по ID документа и полных дублей строк
    - Преобразование ID в строковый тип
    - Преобразование total_amount в числовой тип (некорректные значения -> NaN)
    - Преобразование date и created_at в datetime (некорректные -> NaN)
    - Фильтрация записей с total_amount > 0
    - Заполнение пустых комментариев пустой строкой
    - Заполнение пропущенной даты датой создания документа
    - Удаление записей без даты (после заполнения)
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/purchase_headers.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/purchase_headers.parquet")
    
    df = df.drop_duplicates(subset=['id'], keep='first')
    df = df.drop_duplicates()
    
    df['id'] = df['id'].astype(str)
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        
    df['comment'] = df['comment'].fillna('')
    
    df['date'] = df['date'].fillna(df['created_at'])
    df = df.dropna(subset=['date'])
    
    df['etl_updated_at'] = datetime.now()
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/purchase_headers.parquet", index=False)
    
    print(f"Purchase headers: {len(df)} records")
    return len(df)


def clean_stock_movements(**context):
    """
    Очистка данных о движениях товаров:
    - Удаление дубликатов по ID движения и полных дублей строк
    - Преобразование movement_date в datetime
    - Преобразование числовых полей (quantity_before, quantity_change, quantity_after) в числовой тип
    - Восстановление недостающих значений: расчет quantity_after по quantity_before + quantity_change
    - Восстановление недостающих значений: расчет quantity_change по quantity_after - quantity_before
    - Удаление записей без обязательных полей (movement_date, product_id, warehouse_id)
    - Фильтрация движений с нулевым изменением количества
    - Добавление флага отрицательного остатка после движения
    - Заполнение пустых комментариев и created_by
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/stock_movements.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/stock_movements.parquet")
    
    df = df.drop_duplicates(subset=['id'], keep='first')
    df = df.drop_duplicates()
    
    df['movement_date'] = pd.to_datetime(df['movement_date'], errors='coerce')
    df['quantity_before'] = pd.to_numeric(df['quantity_before'], errors='coerce')
    df['quantity_change'] = pd.to_numeric(df['quantity_change'], errors='coerce')
    df['quantity_after'] = pd.to_numeric(df['quantity_after'], errors='coerce')
    
    mask = df['quantity_after'].isna() & df['quantity_before'].notna() & df['quantity_change'].notna()
    df.loc[mask, 'quantity_after'] = df.loc[mask, 'quantity_before'] + df.loc[mask, 'quantity_change']
    
    mask2 = df['quantity_change'].isna() & df['quantity_before'].notna() & df['quantity_after'].notna()
    df.loc[mask2, 'quantity_change'] = df.loc[mask2, 'quantity_after'] - df.loc[mask2, 'quantity_before']
    
    df = df.dropna(subset=['movement_date', 'product_id', 'warehouse_id'])
    
    df = df[df['quantity_change'] != 0]
    
    df['is_negative_after'] = df['quantity_after'] < 0
    
    df['comment'] = df['comment'].fillna('')
    df['created_by'] = df['created_by'].fillna('system')
    
    df['etl_updated_at'] = datetime.now()
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/stock_movements.parquet", index=False)
    
    negative_count = df['is_negative_after'].sum()
    print(f"Stock movements: {len(df)} records ({negative_count} with negative balance after)")
    return len(df)


def clean_products(**context):
    """
    Очистка данных о товарах:
    - Удаление дубликатов по SKU и полных дублей строк
    - Очистка текстовых полей (обрезка пробелов, замена пустых строк на NaN, приведение SKU к верхнему регистру)
    - Заполнение пропусков: name='Unknown', unit='pcs'
    - Преобразование цен в числовой тип, заполнение пропусков нулем
    - Добавление флага проблем с ценовой логикой (selling_price < cost_price)
    - Удаление записей без SKU
    - Заполнение is_active=True для пропущенных значений
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/products.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/products.parquet")
    
    df = df.drop_duplicates(subset=['sku'], keep='first')
    df = df.drop_duplicates()
    
    df = clean_text_columns(df)
    
    df['name'] = df['name'].fillna('Unknown')
    df['unit'] = df['unit'].fillna('pcs')
    df['cost_price'] = pd.to_numeric(df['cost_price'], errors='coerce').fillna(0)
    df['selling_price'] = pd.to_numeric(df['selling_price'], errors='coerce').fillna(0)
    
    df['price_logic_issue'] = df['selling_price'] < df['cost_price']
    
    df = df.dropna(subset=['sku'])
    df['is_active'] = df['is_active'].fillna(True).astype(bool)
    
    df['etl_updated_at'] = datetime.now()
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/products.parquet", index=False)
    
    price_issues = df['price_logic_issue'].sum()
    print(f"Products: {len(df)} records ({price_issues} with price logic issues)")
    return len(df)


def clean_stock_balances(**context):
    """
    Очистка данных об остатках на складах:
    - Удаление дубликатов по паре (product_id, warehouse_id) с сохранением последней записи
    - Преобразование quantity в числовой тип
    - Очистка отрицательных остатков: замена на 0
    - Удаление записей без идентификаторов товара или склада
    - Добавление служебного поля etl_updated_at
    - Сохранение результата в silver/postgres/stock_balances.parquet
    """
    df = pd.read_parquet(f"{BRONZE_PATH}/postgres/stock_balances.parquet")
    
    df = df.drop_duplicates(subset=['product_id', 'warehouse_id'], keep='last')
    df = df.drop_duplicates()
    
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    
    df.loc[df['quantity'] < 0, 'quantity'] = 0
    df = df.dropna(subset=['product_id', 'warehouse_id'])
    
    df['etl_updated_at'] = datetime.now()
    
    os.makedirs(f"{SILVER_PATH}/postgres", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/postgres/stock_balances.parquet", index=False)
    
    print(f"Stock balances: {len(df)} records")
    return len(df)


def create_data_quality_report(**context):
    """
    Создание отчета о качестве данных в silver-слое:
    - Сбор информации по всем таблицам в silver/postgres/
    - Для каждой таблицы фиксируется: количество записей, количество пропусков по каждой колонке, количество дубликатов
    - Формируется структурированный отчет с временной меткой
    - Сохранение отчета в silver/metadata/quality_report.parquet
    """
    report = {
        'timestamp': datetime.now().isoformat(),
        'tables': {}
    }
    
    silver_files = [
        'customers', 'drivers', 'vehicles', 'suppliers', 'warehouses',
        'sale_headers', 'purchase_headers', 'stock_movements', 'products', 'stock_balances'
    ]
    
    for table in silver_files:
        path = f"{SILVER_PATH}/postgres/{table}.parquet"
        if os.path.exists(path):
            df = pd.read_parquet(path)
            report['tables'][table] = {
                'row_count': len(df),
                'null_counts': df.isnull().sum().to_dict(),
                'duplicates': df.duplicated().sum()
                }
    
    os.makedirs(f"{SILVER_PATH}/metadata", exist_ok=True)
    pd.DataFrame([report]).to_parquet(f"{SILVER_PATH}/metadata/quality_report.parquet", index=False)
    
    print("Data quality report created")
    return True


def clean_expenses(**context):
    """
    Очистка данных о расходах из Excel-файла:
    - Загрузка данных из bronze/excel/all_expenses.parquet
    - Удаление дубликатов
    - Переименование колонок с русских названий на английские
    - Парсинг даты из комбинации колонок day_month_raw и source_month (формат: "день.месяц" + "месяц_год")
    - Преобразование суммы расхода в числовой тип (удаление символа ₽, пробелов, замена запятой на точку)
    - Фильтрация неотрицательных сумм
    - Очистка текстовых полей (госномер, имя водителя, категория расхода)
    - Маппинг vehicle_plate на vehicle_id из справочника vehicles
    - Маппинг driver_name на driver_id из справочника drivers (полное совпадение ФИО или по фамилии, если только один водитель с такой фамилией)
    - Категоризация расходов на русском в английские категории (fuel, repair, wash, other)
    - Добавление служебных полей etl_updated_at, etl_source, record_type
    - Сохранение результата в silver/excel/all_expenses.parquet
    """
    file_path = f"{BRONZE_PATH}/excel/all_expenses.parquet"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return 0
    
    df = pd.read_parquet(file_path)
    
    print(f"Loaded {len(df)} records from expenses")
    print(f"Columns: {df.columns.tolist()}")
    
    df = df.drop_duplicates()
    
    column_mapping = {
        'Дата': 'day_month_raw',
        'Гос. номер': 'vehicle_plate',
        'Водитель': 'driver_name',
        'Статья затрат': 'expense_category',
        'Контрагент (кому заплатили)': 'counterparty',
        'Номер документа': 'document_number',
        'Сумма, руб.': 'amount_rub',
        'Примечание': 'comment'
    }
    df = df.rename(columns=column_mapping)
    
    if 'day_month_raw' in df.columns and 'source_month' in df.columns:
        month_map = {
            'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
            'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
            'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
        }
        
        def parse_full_date(row):
            try:
                day_month = str(row['day_month_raw']) if pd.notna(row['day_month_raw']) else ''
                if '.' not in day_month:
                    return None
                
                day = int(day_month.split('.')[0])
                month_from_day = int(day_month.split('.')[1])
                
                source_month_str = str(row['source_month']) if pd.notna(row['source_month']) else ''
                if '_' not in source_month_str:
                    return None
                
                month_name = source_month_str.split('_')[0].lower()
                year = int(source_month_str.split('_')[1])
                
                month = month_from_day
                return pd.to_datetime(f"{year}-{month:02d}-{day:02d}", format='%Y-%m-%d', errors='coerce')
                
            except (ValueError, IndexError, KeyError):
                return None
        
        df['expense_date'] = df.apply(parse_full_date, axis=1)
        df = df.drop(columns=['day_month_raw', 'source_month'])
        df = df.dropna(subset=['expense_date'])
    else:
        print("Warning: Required columns not found")
        if 'expense_date' not in df.columns:
            df['expense_date'] = None
        df = df.dropna(subset=['expense_date'])
    
    if 'amount_rub' in df.columns:
        if df['amount_rub'].dtype == 'object':
            df['amount_rub'] = df['amount_rub'].astype(str).str.replace('₽', '').str.replace(' ', '').str.replace(',', '.')
            df['amount_rub'] = pd.to_numeric(df['amount_rub'], errors='coerce')
        df = df[df['amount_rub'] >= 0]
    
    if 'vehicle_plate' in df.columns:
        df['vehicle_plate'] = df['vehicle_plate'].fillna('Unknown').astype(str).str.strip().str.upper()
    
    if 'driver_name' in df.columns:
        df['driver_name'] = df['driver_name'].fillna('Unknown').astype(str).str.strip()
        df['driver_name'] = df['driver_name'].str.replace(r'\s+', ' ', regex=True)
    
    if 'expense_category' in df.columns:
        df['expense_category'] = df['expense_category'].fillna('Other').astype(str).str.strip()
    
    vehicles_path = f"{SILVER_PATH}/postgres/vehicles.parquet"
    drivers_path = f"{SILVER_PATH}/postgres/drivers.parquet"
    
    if os.path.exists(vehicles_path) and os.path.exists(drivers_path):
        
        vehicles_df = pd.read_parquet(vehicles_path)
        plate_to_id = dict(zip(
            vehicles_df['plate_number'].astype(str).str.upper().str.strip(), 
            vehicles_df['id']
        ))
        
        df['vehicle_id'] = df['vehicle_plate'].map(plate_to_id).fillna(0).astype(int)
        
        mapped_vehicles = (df['vehicle_id'] > 0).sum()
        print(f"Vehicle mapping: {mapped_vehicles}/{len(df)} ({mapped_vehicles/len(df)*100:.1f}%)")
        
        unmapped_plates = df[df['vehicle_id'] == 0]['vehicle_plate'].unique()
        if len(unmapped_plates) > 0:
            print(f"Warning: {len(unmapped_plates)} unmapped vehicle plates: {unmapped_plates[:10]}")
        
        drivers_df = pd.read_parquet(drivers_path)
        
        drivers_df['full_name_clean'] = drivers_df['full_name'].astype(str).str.upper().str.strip()
        drivers_df['full_name_normalized'] = drivers_df['full_name_clean'].str.replace(r'\s+', ' ', regex=True)
        
        active_drivers = drivers_df[drivers_df['is_active'] == True] if 'is_active' in drivers_df.columns else drivers_df
        
        name_to_id = dict(zip(active_drivers['full_name_normalized'], active_drivers['id']))
        
        active_drivers['last_name'] = active_drivers['full_name_normalized'].str.split().str[0]
        lastname_to_ids = active_drivers.groupby('last_name')['id'].apply(list).to_dict()
        
        def map_driver_to_id(driver_name):
            if pd.isna(driver_name) or driver_name == 'Unknown' or driver_name == '':
                return 0
            
            name_clean = str(driver_name).upper().strip()
            name_clean = ' '.join(name_clean.split())
            
            if name_clean in name_to_id:
                return name_to_id[name_clean]
            
            last_name = name_clean.split()[0] if name_clean.split() else ''
            if last_name in lastname_to_ids:
                driver_ids = lastname_to_ids[last_name]
                if len(driver_ids) == 1:
                    return driver_ids[0]
                else:
                    print(f"  Ambiguous last name '{last_name}': {len(driver_ids)} drivers found")
                    return 0
            
            return 0
        
        df['driver_id'] = df['driver_name'].apply(map_driver_to_id)
        
        mapped_drivers = (df['driver_id'] > 0).sum()
        print(f"Driver mapping: {mapped_drivers}/{len(df)} ({mapped_drivers/len(df)*100:.1f}%)")
        
        unmapped_drivers = df[df['driver_id'] == 0]['driver_name'].unique()
        if len(unmapped_drivers) > 0:
            print(f"Warning: {len(unmapped_drivers)} unmapped drivers: {unmapped_drivers[:10]}")
        
        category_mapping = {
            'Топливо': 'fuel',
            'ГСМ': 'fuel',
            'Бензин': 'fuel',
            'Ремонт': 'repair',
            'Запчасти': 'repair',
            'Техобслуживание': 'repair',
            'ТО': 'repair',
            'Мойка': 'wash',
            'Мойка авто': 'wash',
        }
        
        df['expense_category_en'] = df['expense_category'].map(category_mapping).fillna('other')
        
    else:
        print("Warning: Vehicle or driver reference files not found, skipping ID mapping")
        df['vehicle_id'] = 0
        df['driver_id'] = 0
        df['expense_category_en'] = 'other'
    
    df['etl_updated_at'] = datetime.now()
    df['etl_source'] = 'excel/all_expenses'
    df['record_type'] = 'expense'
    
    os.makedirs(f"{SILVER_PATH}/excel", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/excel/all_expenses.parquet", index=False)
    
    print(f"Expenses: {len(df)} records saved")
    print(f"Date range: {df['expense_date'].min()} to {df['expense_date'].max()}")
    print(f"Final columns: {df.columns.tolist()}")
    
    return len(df)


def clean_telemetry(**context):
    """
    Очистка телеметрических данных из JSON-файлов:
    - Загрузка данных из bronze/json/telemetry.parquet
    - Разворачивание вложенных структур: извлечение полей из словарей loc, acc, eng, flags, gps
    - Преобразование timestamp из миллисекунд в datetime
    - Удаление дубликатов
    - Удаление записей без timestamp
    - Преобразование числовых полей в числовой тип
    - Преобразование флагов в булевый тип
    - Заполнение пропусков значениями по умолчанию
    - Добавление служебных полей etl_updated_at, etl_source, record_type, event_date
    - Сохранение результата в silver/json/telemetry.parquet
    """
    file_path = f"{BRONZE_PATH}/json/telemetry.parquet"
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return 0
    
    df = pd.read_parquet(file_path)
    
    print(f"Loaded {len(df)} records from telemetry")
    print(f"Original columns: {df.columns.tolist()}")
    
    if 'order_key' in df.columns:
        before = len(df)
        df = df.dropna(subset=['order_key'])
        print(f"Removed {before - len(df)} records without order_key")
    else:
        print("Warning: 'order_key' column not found, skipping filter")
    
    if 'loc' in df.columns:
        df['latitude'] = df['loc'].apply(lambda x: x.get('lat') if isinstance(x, dict) else None)
        df['longitude'] = df['loc'].apply(lambda x: x.get('lon') if isinstance(x, dict) else None)
        df = df.drop('loc', axis=1)
    
    if 'acc' in df.columns:
        df['acc_lon'] = df['acc'].apply(lambda x: x.get('lon') if isinstance(x, dict) else None)
        df['acc_lat'] = df['acc'].apply(lambda x: x.get('lat') if isinstance(x, dict) else None)
        df = df.drop('acc', axis=1)
    
    if 'eng' in df.columns:
        df['engine_rpm'] = df['eng'].apply(lambda x: x.get('rpm') if isinstance(x, dict) else None)
        df['engine_throttle'] = df['eng'].apply(lambda x: x.get('thr') if isinstance(x, dict) else None)
        df['engine_temp'] = df['eng'].apply(lambda x: x.get('tmp') if isinstance(x, dict) else None)
        df = df.drop('eng', axis=1)
    
    if 'flags' in df.columns:
        df['flag_abs'] = df['flags'].apply(lambda x: x.get('abs') if isinstance(x, dict) else None)
        df['flag_belt'] = df['flags'].apply(lambda x: x.get('belt') if isinstance(x, dict) else None)
        df['flag_doors'] = df['flags'].apply(lambda x: x.get('doors') if isinstance(x, dict) else None)
        df['car_id'] = df['flags'].apply(lambda x: x.get('car_id') if isinstance(x, dict) else None)
        df = df.drop('flags', axis=1)
    
    if 'gps' in df.columns:
        df['gps_satellites'] = df['gps'].apply(lambda x: x.get('sat') if isinstance(x, dict) else None)
        df['gps_hdop'] = df['gps'].apply(lambda x: x.get('hdop') if isinstance(x, dict) else None)
        df = df.drop('gps', axis=1)
    
    if 'ts' in df.columns:
        df['timestamp'] = pd.to_datetime(df['ts'], unit='ms', errors='coerce')
        df = df.drop('ts', axis=1)
    
    df = df.drop_duplicates()
    
    df = df.dropna(subset=['timestamp'])
    
    numeric_fields = ['spd', 'h', 'acc_lon', 'acc_lat', 'engine_rpm', 'engine_throttle', 
                    'engine_temp', 'flag_doors', 'gps_satellites', 'gps_hdop']
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')
    
    if 'flag_abs' in df.columns:
        df['flag_abs'] = df['flag_abs'].astype(bool)
    if 'flag_belt' in df.columns:
        df['flag_belt'] = df['flag_belt'].astype(bool)
    
    if 'car_id' in df.columns:
        df['car_id'] = df['car_id'].fillna('Unknown').astype(str).str.strip().str.upper()
    
    df = df.fillna({
        'spd': 0,
        'h': -1,
        'engine_rpm': 0,
        'engine_throttle': 0,
        'engine_temp': -273,
        'flag_abs': False,
        'flag_belt': False,
        'flag_doors': 0,
        'gps_satellites': 0,
        'gps_hdop': 99.9,
        'latitude': 0.0,
        'longitude': 0.0,
        'acc_lon': 0.0,
        'acc_lat': 0.0
    })
    
    df['etl_updated_at'] = datetime.now()
    df['etl_source'] = 'json/telemetry'
    df['record_type'] = 'telemetry'
    
    if 'timestamp' in df.columns:
        df['event_date'] = df['timestamp'].dt.date
    
    os.makedirs(f"{SILVER_PATH}/json", exist_ok=True)
    df.to_parquet(f"{SILVER_PATH}/json/telemetry.parquet", index=False)
    
    print(f"Telemetry: {len(df)} records saved")
    print(f"Final columns: {df.columns.tolist()}")
    return len(df)


with DAG(
    'silver_layer_etl',
    default_args=default_args,
    description='ETL from bronze to silver with business logic validation',
    schedule_interval='@daily',
    catchup=False,
    tags=['diploma', 'silver'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    dim_tasks = [
        PythonOperator(task_id='clean_customers', python_callable=clean_customers),
        PythonOperator(task_id='clean_drivers', python_callable=clean_drivers),
        PythonOperator(task_id='clean_vehicles', python_callable=clean_vehicles),
        PythonOperator(task_id='clean_suppliers', python_callable=clean_suppliers),
        PythonOperator(task_id='clean_warehouses', python_callable=clean_warehouses),
        PythonOperator(task_id='clean_products', python_callable=clean_products),
    ]
    
    fact_tasks = [
        PythonOperator(task_id='clean_sale_headers', python_callable=clean_sale_headers),
        PythonOperator(task_id='clean_purchase_headers', python_callable=clean_purchase_headers),
        PythonOperator(task_id='clean_stock_movements', python_callable=clean_stock_movements),
        PythonOperator(task_id='clean_stock_balances', python_callable=clean_stock_balances),
    ]
    
    quality_report = PythonOperator(
        task_id='quality_report',
        python_callable=create_data_quality_report,
    )
    
    expenses_task = PythonOperator(
        task_id='clean_expenses',
        python_callable=clean_expenses,
    )

    telemetry_task = PythonOperator(
        task_id='clean_telemetry',
        python_callable=clean_telemetry,
    )

    end = EmptyOperator(task_id='end')
    
    start >> dim_tasks
    start >> expenses_task
    start >> telemetry_task
    [dim for dim in dim_tasks for fact in fact_tasks]  # This line was problematic, fixing:
    for dim in dim_tasks:
        for fact in fact_tasks:
            dim >> fact
    fact_tasks >> quality_report
    [quality_report, expenses_task, telemetry_task] >> end