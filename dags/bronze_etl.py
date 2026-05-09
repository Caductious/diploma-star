from datetime import datetime
import json
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


default_args = {
    'owner': 'Loginov Petr',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'bronze_load',
    default_args=default_args,
    description='Загрузка данных из 3 источников в Bronze слой (Parquet)',
    schedule_interval='@daily',
    catchup=False,
)


def load_postgres_to_bronze():
    """Данная функция извлекает данные из операционной БД
    После чего каждая таблица сохраняется в формате .parquet
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_diploma')
    
    tables = ['customers', 'drivers', 'products', 'purchase_headers',
              'purchase_items', 'sale_headers', 'sale_items', 'stock_balances', 
               'stock_movements', 'suppliers', 'vehicles',  'warehouses']
    
    for table in tables:
        try:
            df = pg_hook.get_pandas_df(f"SELECT * FROM {table}")
            path = f"/opt/airflow/data/bronze/postgres/{table}.parquet"
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(path, index=False)
            print(f"PostgreSQL {table}: {len(df)} строк -> {path}")
        except Exception as e:
            print(f"Ошибка в таблице {table}: {e}")


def load_excel_to_bronze():
    """Данная функция извлекает файлы с данными о затратах на автомобили
    после этого данные из них сохраняются в один .parquet файл
    кроме того удаляется последняя строка ИТОГО т.к она не имеет важности
    и не вписывается в логику обработки данных"""
    excel_dir = "/opt/airflow/data/raw/excel"
    all_dfs = []
    
    excel_files = list(Path(excel_dir).glob("*.xlsx"))
    
    if not excel_files:
        print("! Excel файлы не найдены")
        return
    
    for file in excel_files:
        try:
            df = pd.read_excel(file)
            df['source_file'] = file.name
            df['source_month'] = file.stem
            df['load_date'] = datetime.now().isoformat()
            all_dfs.append(df)
            print(f"Excel {file.name}: {len(df)} строк")
        except Exception as e:
            print(f"Ошибка в файле {file.name}: {e}")
    
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        combined_df = combined_df[combined_df['Номер документа'] != 'ИТОГО:']
        combined_df = combined_df.dropna(how='all')
        
        output_path = "/opt/airflow/data/bronze/excel/all_expenses.parquet"
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        combined_df.to_parquet(output_path, index=False)
        print(f"Excel: загружено {len(combined_df)} строк из {len(all_dfs)} файлов -> {output_path}")
        

def load_json_to_bronze():
    """Данная функция извлекает информацию из JSONL логов,
    после чего эта информация записывается в файл telemetry.parquet"""
    json_base = Path("/opt/airflow/data/raw/json")
    json_files = list(json_base.glob("dt=*/*.jsonl"))
    if not json_files:
        print("! JSON файлы не найдены")
        return
    
    records = []
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.strip():
                        data = json.loads(line)
                        data['source_file'] = json_file.name  # Path дает .name
                        # Извлекаем дату из пути
                        date_part = json_file.parent.name.replace('dt=', '')
                        data['log_date'] = date_part
                        data['load_timestamp'] = datetime.now().isoformat()
                        records.append(data)
            print(f"JSON {json_file.name}: обработан")
        except Exception as e:
            print(f"Ошибка в файле {json_file}: {e}")
    
    if records:
        df = pd.DataFrame(records)
        
        if 'loc' in df.columns:
            df['lat'] = df['loc'].apply(lambda x: x.get('lat') if isinstance(x, dict) else None)
            df['lon'] = df['loc'].apply(lambda x: x.get('lon') if isinstance(x, dict) else None)
        
        if 'eng' in df.columns:
            df['rpm'] = df['eng'].apply(lambda x: x.get('rpm') if isinstance(x, dict) else None)
            df['engine_temp'] = df['eng'].apply(lambda x: x.get('tmp') if isinstance(x, dict) else None)
        
        if 'flags' in df.columns:
            df['car_id'] = df['flags'].apply(lambda x: x.get('car_id') if isinstance(x, dict) else None)
            df['abs_active'] = df['flags'].apply(lambda x: x.get('abs') if isinstance(x, dict) else None)
        
        if 'gps' in df.columns:
            df['gps_satellites'] = df['gps'].apply(lambda x: x.get('sat') if isinstance(x, dict) else None)
        
        output_path = "/opt/airflow/data/bronze/json/telemetry.parquet"
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_path, index=False)
        unique_files = len(set([r['source_file'] for r in records]))
        print(f"JSON: обработано {len(records)} записей из {unique_files} файлов -> {output_path}")
    else:
        print("Ошибка Нет данных в JSON файлах")


t1 = PythonOperator(
    task_id='postgres_to_bronze',
    python_callable=load_postgres_to_bronze,
    dag=dag
)

t2 = PythonOperator(
    task_id='excel_to_bronze',
    python_callable=load_excel_to_bronze,
    dag=dag
)

t3 = PythonOperator(
    task_id='json_to_bronze',
    python_callable=load_json_to_bronze,
    dag=dag
)