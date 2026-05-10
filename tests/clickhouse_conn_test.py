from clickhouse_driver import Client
from dotenv import load_dotenv
import os 

load_dotenv()

def test_connection():
    print("Проверка подключения к ClickHouse...")
    print(f"   Хост: {os.getenv('CLICKHOUSEHOST')}:{os.getenv('CLICKHOUSEPORT')}")
    print(f"   Пользователь: {os.getenv('CLICKHOUSEUSER')}")
    
    try:
            # Подключение
        client = Client(
        host=os.getenv('CLICKHOUSEHOST'),
        port=int(os.getenv('CLICKHOUSEPORT')),
        user=os.getenv('CLICKHOUSEUSER'),
        password=os.getenv('CLICKHOUSEPASSWORD')
        )        
        # Простой запрос
        result = client.execute("SELECT version()")
        version = result[0][0]
        print(f"\nПОДКЛЮЧЕНИЕ УСПЕШНО!")
        print(f"   Версия ClickHouse: {version}")
        
        # Проверка прав на создание БД
        client.execute("CREATE DATABASE IF NOT EXISTS test_db")
        client.execute("DROP DATABASE test_db")
        print("   Права на запись: Есть")
        
        print("\nClickHouse готов к работе!")
        return True
        
    except Exception as e:
        print(f"\nОШИБКА ПОДКЛЮЧЕНИЯ!")
        print(f"   {str(e)}")
        print("\nВозможные причины:")
        print("   1. ClickHouse не запущен")
        print("   2. Неправильный хост или порт")
        print("   3. Неправильный пользователь или пароль")
        print("\nПроверь:")
        print("   - Запущен ли сервер: sudo systemctl status clickhouse-server")
        print("   - Порт: netstat -tlnp | grep 9000")
        return False

if __name__ == "__main__":
    test_connection()