from datetime import datetime
from dotenv import load_dotenv
import os

from log_generator import TelemetryGenerator
from populate_db import DatabaseGenerator
from xlsx_generator import XlsxGenerator


load_dotenv()

DB_CONFIG = {
    'dbname': os.getenv("PGDBNAME"),
    'user': os.getenv('PGUSER'),
    'password': os.getenv('PGPASSWORD'),
    'host': os.getenv('PGHOST'),
    'port': os.getenv('PGPORT')
}

os.makedirs('data/raw/json')
os.makedirs('data/raw/excel')


db_generator = DatabaseGenerator(db_config=DB_CONFIG)
db_generator.execute()

telemetry_generator = TelemetryGenerator(db_config=DB_CONFIG, base_path="data/raw/json")
telemetry_generator.generate_all_deliveries(
        days_back=365,
        max_deliveries=1000,  
        interval_sec=30
    )

excel_generator = XlsxGenerator(db_config=DB_CONFIG)
start_date = datetime(2024, 5, 1)
end_date = datetime(2026, 4, 30)  
excel_generator.generate_report_for_period(
        start_date=start_date,
        end_date=end_date,
        records_per_vehicle=6,
        output_dir="data/raw/excel"
    )