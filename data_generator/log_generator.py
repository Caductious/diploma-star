import json
import random
import psycopg2
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List, Dict, Any

class TelemetryGenerator:
    def __init__(self, db_config: Dict[str, str], base_path: str = "data/raw/json"):
        self.db_config = db_config
        self.base_path = Path(base_path)
        self.conn = None
        
    def connect_db(self):
        self.conn = psycopg2.connect(**self.db_config)
        return self.conn
    
    def close_db(self):
        if self.conn:
            self.conn.close()
    
    def get_deliveries_from_db(self, days_back: int = 30) -> List[Dict[str, Any]]:
        """Получение списка доставок из БД"""
        deliveries = []
        
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    sh.id as sale_id,
                    sh.number as sale_number,
                    sh.created_at,
                    v.plate_number
                FROM sale_headers sh
                LEFT JOIN vehicles v ON v.id = sh.vehicle_id
                WHERE sh.vehicle_id IS NOT NULL
                  AND sh.driver_id IS NOT NULL
                  AND sh.date >= CURRENT_DATE - INTERVAL %s
                ORDER BY sh.date DESC
            """, (f'{days_back} days',))
            
            for row in cur.fetchall():
                deliveries.append({
                    'sale_id': row[0],
                    'sale_number': row[1],
                    'created_at': row[2],
                    'plate_number': row[3]
                })
        
        print(f"Найдено {len(deliveries)} доставок")
        return deliveries
    
    def generate_log(self, plate_number: str, timestamp_ms: int, trip_progress: float = 0.5) -> Dict[str, Any]:
        """Генерация одной записи телеметрии"""
        if trip_progress < 0.1:
            speed = trip_progress * 10 * 60
        elif trip_progress > 0.9:
            speed = (1 - trip_progress) * 10 * 60
        else:
            speed = random.uniform(50, 80)
        
        return {
            "ts": timestamp_ms,
            "loc": {
                "lat": 55.751244 + random.uniform(-0.05, 0.05) * trip_progress,
                "lon": 37.618423 + random.uniform(-0.05, 0.05) * trip_progress
            },
            "spd": round(speed, 1),
            "h": random.randint(0, 359),
            "acc": {
                "lon": round(random.uniform(-1, 1), 2),
                "lat": round(random.uniform(-1, 1), 2)
            },
            "eng": {
                "rpm": random.randint(800, 3000) if speed < 80 else random.randint(2000, 5000),
                "thr": random.randint(10, 90) if speed > 0 else 0,
                "tmp": random.randint(70, 110)
            },
            "flags": {
                "abs": random.choice([True, False]),
                "belt": True,
                "doors": 0 if speed > 0 else random.choice([0, 1, 2]),
                "car_id": plate_number
            },
            "gps": {
                "sat": random.randint(6, 15),
                "hdop": round(random.uniform(0.8, 2.5), 1)
            }
        }
    
    def generate_trip_logs(self, delivery: Dict[str, Any], 
                          duration_hours: float = None,
                          interval_sec: int = 2) -> Path:
        """Генерация логов для конкретной доставки"""
        plate_number = delivery['plate_number']
        sale_id = delivery['sale_id']
        
        start_time = delivery['created_at'] or datetime.now()
        if isinstance(start_time, date) and not isinstance(start_time, datetime):
            start_time = datetime.combine(start_time, datetime.min.time())
        
        if duration_hours is None:
            duration_hours = random.uniform(1, 4)
        
        end_time = start_time + timedelta(hours=duration_hours)
        
        current = start_time
        logs = []
        
        while current <= end_time:
            progress = (current - start_time).total_seconds() / (duration_hours * 3600)
            log = self.generate_log(plate_number, int(current.timestamp() * 1000), progress)
            logs.append(log)
            current += timedelta(seconds=interval_sec)
        
        # Сохраняем в файл
        date_path = self.base_path / f"dt={start_time.strftime('%Y-%m-%d')}"
        date_path.mkdir(parents=True, exist_ok=True)
        
        # Имя файла содержит sale_id для связи с БД
        filename = f"delivery_{sale_id}_{plate_number}_{start_time.strftime('%Y%m%d_%H%M%S')}.jsonl"
        filepath = date_path / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            for log in logs:
                f.write(json.dumps(log, ensure_ascii=False) + '\n')
        
        #print(f"  Сгенерировано {len(logs)} записей для доставки {sale_id} ({plate_number})")
        return filepath
    
    def generate_all_deliveries(self, days_back: int = 30, 
                               max_deliveries: int = None,
                               interval_sec: int = 30) -> List[Path]:
        """Генерация логов для всех доставок из БД"""
        self.connect_db()
        
        try:
            deliveries = self.get_deliveries_from_db(days_back)
            
            if max_deliveries:
                deliveries = deliveries[:max_deliveries]
            
            print(f"\nГенерация логов для {len(deliveries)} доставок...")
            
            generated_files = []
            for i, delivery in enumerate(deliveries, 1):
                print(f"[{i}/{len(deliveries)}] Доставка {delivery['sale_id']}:")
                filepath = self.generate_trip_logs(delivery, interval_sec=interval_sec)
                generated_files.append(filepath)
            
            print(f"\nГотово! Создано файлов: {len(generated_files)}")
            return generated_files
            
        finally:
            self.close_db()

