import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

class DatabaseGenerator:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
        self.NUM_WAREHOUSES = 3
        self.NUM_PRODUCTS = 100
        self.NUM_CUSTOMERS = 50
        self.NUM_SUPPLIERS = 20
        self.NUM_DRIVERS = 15
        self.NUM_VEHICLES = 10
        self.NUM_PURCHASES = 500
        self.NUM_SALES = 700
        self.MAX_ITEMS_PER_DOCUMENT = 15
        self.MIN_ITEMS_PER_DOCUMENT = 1

        Faker.seed(42)
        random.seed(42)

        self.fake = Faker('ru_RU')

    def get_connection(self):
        #подключение к БД
        try:
            conn = psycopg2.connect(**self.db_config)
            print("Успешное подключение к базе данных")
            return conn
        except Exception as e:
            print(f"Ошибка подключения: {e}")
            raise

    def clear_tables(self, conn):
        """Clear all tables in correct order (respecting foreign keys)"""
        print("Очистка...")
        with conn.cursor() as cur:
            cur.execute("""
                TRUNCATE TABLE 
                    sale_items,
                    purchase_items,
                    sale_headers,
                    purchase_headers,
                    stock_movements,
                    stock_balances,
                    vehicles,
                    drivers,
                    suppliers,
                    customers,
                    products,
                    warehouses
                RESTART IDENTITY CASCADE;
            """)
        conn.commit()
        print("Таблицы очищены.")

    def generate_russian_license_plate(self):
        letters = ['А', 'В', 'Е', 'К', 'М', 'Н', 'О', 'Р', 'С', 'Т', 'У', 'Х']
        # Формат: Буква + 3 цифры + 2 буквы + код региона (от 01 до 199)
        letter1 = random.choice(letters)
        numbers = f"{random.randint(100, 999)}"
        letter2 = random.choice(letters)
        letter3 = random.choice(letters)
        region = random.randint(1, 199)
        return f"{letter1}{numbers}{letter2}{letter3} {region:03d}"

    def generate_russian_driver_license(self):
        # Формат: 2 цифры (серия) + 2 буквы + 6 цифр
        series = f"{random.randint(10, 99):02d}"
        letters = ['А', 'В', 'Е', 'К', 'М', 'Н', 'О', 'Р', 'С', 'Т', 'У', 'Х']
        letter_pair = f"{random.choice(letters)}{random.choice(letters)}"
        number = f"{random.randint(100000, 999999)}"
        return f"{series} {letter_pair} {number}"

    def generate_warehouses(self, conn, count):
        print(f"Создание {count} складов...")
        warehouses = []
        for i in range(count):
            warehouses.append((
                f"Склад {self.fake.city_prefix()} {i+1}",
                self.fake.street_address(),
                True
                )) 
        ids = []
        with conn.cursor() as cur:
            for warehouse in warehouses:
                try:
                    cur.execute("""
                        INSERT INTO warehouses (name, address, is_active)
                        VALUES (%s, %s, %s) RETURNING id;
                    """, warehouse)
                    result = cur.fetchone()
                    if result:
                        ids.append(result[0])
                except Exception as e:
                    print(f"ОШИБКА добавления склада: {e}")
                    conn.rollback()
                    continue
            conn.commit()
        print(f"Создано {len(ids)} складов")
        if len(ids) == 0:
            print("ВНИМАНИЕ НЕТ СКЛАДОВ, проверьте существование базы")
        return ids

    def generate_products(self, conn, count):
        """Generate products"""
        print(f"Создание {count} продуктов...")
        products = []
        units = ['шт', 'кг', 'л', 'м', 'уп', 'кор', 'пал']
        
        for i in range(count):
            product_types = ['Колбаса', 'Сыр', 'Молоко', 'Хлеб', 'Масло', 'Йогурт', 
                            'Печенье', 'Конфеты', 'Сок', 'Вода', 'Пиво', 'Чай', 'Кофе']
            product_name = f"{random.choice(product_types)} {self.fake.word()} {self.fake.random_number(digits=2)}"
            
            products.append((
                f"SKU-{self.fake.unique.random_number(digits=8)}",
                product_name,
                random.choice(units),
                round(random.uniform(50, 5000), 2),
                round(random.uniform(100, 10000), 2),
                True
            ))
        ids = []
        with conn.cursor() as cur:
            for product in products:
                try:
                    cur.execute("""
                        INSERT INTO products (sku, name, unit, cost_price, selling_price, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
                    """, product)
                    result = cur.fetchone()
                    if result:
                        ids.append(result[0])
                except Exception as e:
                    print(f"ОШИБКА добавления продукта: {e}")
                    conn.rollback()
                    continue
            conn.commit()
        
        print(f"Создано {len(ids)} продуктов")
        return ids

    def generate_customers(self, conn, count):
        """Generate customers"""
        print(f"Создание {count} клиентов...")
        customers = []
        for _ in range(count):
            customers.append((
                self.fake.company(),
                self.fake.unique.bothify(text='##########', letters='0123456789'),
                f"Тел: {self.fake.phone_number()}\nEmail: {self.fake.email()}\nКонтакт: {self.fake.name()}",
                self.fake.street_address(),
                True
            ))
        ids = []
        with conn.cursor() as cur:
            for customer in customers:
                try:
                    cur.execute("""
                        INSERT INTO customers (name, inn, contacts, address, is_active)
                        VALUES (%s, %s, %s, %s, %s) RETURNING id;
                    """, customer)
                    result = cur.fetchone()
                    if result:
                        ids.append(result[0])
                except Exception as e:
                    print(f"ОШИБКА добавления клиента: {e}")
                    conn.rollback()
                    continue
            conn.commit()
        
        print(f"Создано {len(ids)} клиентов")
        return ids

    def generate_suppliers(self, conn, count):
        print(f"Создание {count} поставщиков...")
        suppliers = []
        for _ in range(count):
            suppliers.append((
                self.fake.company(),
                self.fake.unique.bothify(text='##########', letters='0123456789'),
                f"Тел: {self.fake.phone_number()}\nEmail: {self.fake.email()}\nКонтакт: {self.fake.name()}",
                True
            ))
        
        ids = []
        with conn.cursor() as cur:
            for supplier in suppliers:
                try:
                    cur.execute("""
                        INSERT INTO suppliers (name, inn, contacts, is_active)
                        VALUES (%s, %s, %s, %s) RETURNING id;
                    """, supplier)
                    result = cur.fetchone()
                    if result:
                        ids.append(result[0])
                except Exception as e:
                    print(f"ОШИБКА добавления поставщика: {e}")
                    conn.rollback()
                    continue
            conn.commit()
        
        print(f"Создано {len(ids)} поставщиков")
        return ids

    def generate_drivers(self, conn, count):
        print(f"Генерация {count} водителей...")
        drivers = []
        for _ in range(count):
            drivers.append((
                self.fake.name(),
                self.generate_russian_driver_license(),
                self.fake.phone_number(),
                True
            ))
        
        ids = []
        with conn.cursor() as cur:
            for driver in drivers:
                try:
                    cur.execute("""
                        INSERT INTO drivers (full_name, license_number, phone, is_active)
                        VALUES (%s, %s, %s, %s) RETURNING id;
                    """, driver)
                    result = cur.fetchone()
                    if result:
                        ids.append(result[0])
                except Exception as e:
                    print(f"ОШИБКА добавления водителя: {e}")
                    conn.rollback()
                    continue
            conn.commit()
        
        print(f"Создано {len(ids)} водителей")
        return ids

    def generate_vehicles(self, conn, count):
        """Generate vehicles without driver binding"""
        print(f"Создано {count} ТС...")
        vehicles = []
        models = ['ГАЗель NEXT', 'КамАЗ 5490', 'MAN TGS', 'Volvo FH', 'Scania R450',
                'Mercedes Actros', 'Renault Magnum', 'ISUZU ELF', 'Hyundai HD78']
        
        for _ in range(count):
            vehicles.append((
                self.generate_russian_license_plate(),  # Российский формат номера
                random.choice(models),
                True
            ))
        
        ids = []
        with conn.cursor() as cur:
            for vehicle in vehicles:
                try:
                    cur.execute("""
                        INSERT INTO vehicles (plate_number, model, is_active)
                        VALUES (%s, %s, %s) RETURNING id;
                    """, vehicle)
                    result = cur.fetchone()
                    if result:
                        ids.append(result[0])
                except Exception as e:
                    print(f"ОШИБКА добавления транспорта: {e}")
                    conn.rollback()
                    continue
            conn.commit()
        
        print(f"Создано {len(ids)} сущностей ТС")
        return ids

    def generate_purchases(self, conn, suppliers_ids, products_ids, warehouses_ids, count):
        """Generate purchase documents with items"""
        print(f"Создание {count} покупок...")
        purchase_ids = []
        start_date = datetime.now() - timedelta(days=365)
        
        for i in range(count):
            doc_date = self.fake.date_between(start_date=start_date, end_date='now')
            
            # Insert header
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO purchase_headers 
                        (number, date, supplier_id, warehouse_id, comment, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id;
                    """, (
                        f"PURCH-{doc_date.strftime('%Y%m%d')}-{i+1:04d}",
                        doc_date,
                        random.choice(suppliers_ids),
                        random.choice(warehouses_ids),
                        self.fake.sentence(nb_words=10) if random.random() > 0.7 else None,
                        self.fake.date_time_between(start_date=doc_date, end_date='now')
                    ))
                    purchase_id = cur.fetchone()[0]
                    purchase_ids.append(purchase_id)
            except Exception as e:
                print(f"Ошибка доавления заголовка продажи {i+1}: {e}")
                conn.rollback()
                continue
            
            # Generate items
            num_items = random.randint(self.MIN_ITEMS_PER_DOCUMENT, min(self.MAX_ITEMS_PER_DOCUMENT, len(products_ids)))
            selected_products = random.sample(products_ids, num_items)
            
            items = []
            for product_id in selected_products:
                quantity = random.randint(1, 100)
                purchase_price = round(random.uniform(50, 5000), 2)
                items.append((purchase_id, product_id, quantity, purchase_price))
            
            # Insert items
            try:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO purchase_items (purchase_id, product_id, quantity, purchase_price)
                        VALUES (%s, %s, %s, %s);
                    """, items)
                conn.commit()
            except Exception as e:
                print(f"ОШИБКА добавления продажи для документа {i+1}: {e}")
                conn.rollback()
                continue
            
            if (i + 1) % 50 == 0:
                print(f"  Сгенерированно {i + 1}/{count} покупок")
        
        print(f"Создано {len(purchase_ids)} покупок")
        return purchase_ids

    def generate_sales(self, conn, customers_ids, products_ids, warehouses_ids, 
                    drivers_ids, vehicles_ids, count):
        #Создание сведений о продажах
        print(f"Созание {count} документов о продаже...")
        sale_ids = []
        start_date = datetime.now() - timedelta(days=365)
        
        successful_sales = 0
        attempts = 0
        max_attempts = count * 3
        
        while successful_sales < count and attempts < max_attempts:
            attempts += 1
            doc_date = self.fake.date_between(start_date=start_date, end_date='now')
            
            # Insert header (driver_id и vehicle_id теперь независимы друг от друга)
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO sale_headers 
                        (number, date, customer_id, driver_id, vehicle_id, warehouse_id, comment, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
                    """, (
                        f"SALE-{doc_date.strftime('%Y%m%d')}-{successful_sales+1:04d}",
                        doc_date,
                        random.choice(customers_ids),
                        random.choice(drivers_ids) if drivers_ids else None,
                        random.choice(vehicles_ids) if vehicles_ids else None,
                        random.choice(warehouses_ids),
                        self.fake.sentence(nb_words=10) if random.random() > 0.7 else None,
                        self.fake.date_time_between(start_date=doc_date, end_date='now')
                    ))
                    sale_id = cur.fetchone()[0]
            except Exception as e:
                print(f"ОШИБКА добавления заголовка продажи: {e}")
                conn.rollback()
                continue
            
            # Generate items
            num_items = random.randint(self.MIN_ITEMS_PER_DOCUMENT, min(self.MAX_ITEMS_PER_DOCUMENT, len(products_ids)))
            selected_products = random.sample(products_ids, min(num_items, len(products_ids)))
            
            items = []
            for product_id in selected_products:
                quantity = random.randint(1, 30)
                selling_price = round(random.uniform(100, 10000), 2)
                items.append((sale_id, product_id, quantity, selling_price))
            
            try:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO sale_items (sale_id, product_id, quantity, selling_price)
                        VALUES (%s, %s, %s, %s);
                    """, items)
                conn.commit()
                sale_ids.append(sale_id)
                successful_sales += 1
                
                if successful_sales % 50 == 0:
                    print(f"  Создано {successful_sales}/{count} документов о продаже")
                    
            except Exception as e:
                conn.rollback()
                # Delete the header
                try:
                    with conn.cursor() as cur:
                        cur.execute("DELETE FROM sale_headers WHERE id = %s", (sale_id,))
                    conn.commit()
                except:
                    pass
                continue
        
        print(f"Создано {len(sale_ids)} документов о продаже")
        return sale_ids

    def generate_initial_stock_balances(self, conn, products_ids, warehouses_ids):
        print("Создание баланса складов...")
        balances = []
        
        for product_id in products_ids:
            for warehouse_id in warehouses_ids:
                initial_qty = random.randint(0, 500)
                if initial_qty > 0:
                    balances.append((product_id, warehouse_id, initial_qty))
        
        if balances:
            with conn.cursor() as cur:
                try:
                    cur.executemany("""
                        INSERT INTO stock_balances (product_id, warehouse_id, quantity)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (product_id, warehouse_id) DO UPDATE 
                        SET quantity = EXCLUDED.quantity;
                    """, balances)
                    conn.commit()
                    print(f"Создано {len(balances)} баланса")
                except Exception as e:
                    print(f"ОШИБКА добавления баланса складов: {e}")
                    conn.rollback()
        else:
            print("ОШИБКА не создан баланс складов")

# основная функция

    def execute(self):
        print("\nСОЗДАНИЕ ОПЕРАЦИОННОЙ БАЗЫ ДАННЫХ POSTGRESQL\n")
        
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = 'warehouses'
                """)
                if not cur.fetchone():
                    print("База данных не найдена... Запускаю скрипт создания")
                    with open('data_generator/creation_database_postgres.sql', 'r', encoding='utf-8') as f:
                        sql_script = f.read()
                        cur.execute(sql_script)
                        print("База данных успешно создана!")
            
            self.clear_tables(conn)
            
            print("\nСОЗДАНИЕ ДАННЫХ\n")
            
            warehouses_ids = self.generate_warehouses(conn,self.NUM_WAREHOUSES)
            if not warehouses_ids:
                print("ОШИБКА: Нет складов. Невозможно продолжить")
                return
                
            products_ids = self.generate_products(conn, self.NUM_PRODUCTS)
            customers_ids = self.generate_customers(conn, self.NUM_CUSTOMERS)
            suppliers_ids = self.generate_suppliers(conn, self.NUM_SUPPLIERS)
            drivers_ids = self.generate_drivers(conn, self.NUM_DRIVERS)
            vehicles_ids = self.generate_vehicles(conn, self.NUM_VEHICLES)  # Убрал drivers_ids
            
            self.generate_initial_stock_balances(conn, products_ids, warehouses_ids)
            
            print("\n" + "=" * 40)
            print("СОЗДАНИЕ ДОКУМЕНТОВ ПРОДАЖ")
            print("=" * 40)
            
            if suppliers_ids and products_ids and warehouses_ids:
                purchase_ids = self.generate_purchases(conn, suppliers_ids, products_ids, 
                                                warehouses_ids, self.NUM_PURCHASES)
            else:
                print("Skipping purchases - missing required data")
                purchase_ids = []
            
            if customers_ids and products_ids and warehouses_ids and drivers_ids and vehicles_ids:
                sale_ids = self.generate_sales(conn, customers_ids, products_ids, warehouses_ids,
                                        drivers_ids, vehicles_ids, self.NUM_SALES)
            else:
                print("Skipping sales - missing required data")
                sale_ids = []
            
            # Final summary
            print("\nГЕНЕРАЦИЯ ЗАВЕРШЕНА\n")
            print(f"Итого::")
            print(f"  - Складов: {len(warehouses_ids)}")
            print(f"  - Продуктов: {len(products_ids)}")
            print(f"  - Клиентов: {len(customers_ids)}")
            print(f"  - Поставщиков: {len(suppliers_ids)}")
            print(f"  - Водителей: {len(drivers_ids)}")
            print(f"  - Транспорта: {len(vehicles_ids)}")
            print(f"  - Данных о закупках: {len(purchase_ids)}")
            print(f"  - Данных о продажах: {len(sale_ids)}")
            
            print("\nПримеры сгенерированных данных\n")
            if drivers_ids:
                with conn.cursor() as cur:
                    cur.execute("SELECT license_number FROM drivers LIMIT 1")
                    example_license = cur.fetchone()[0]
                    print(f"Driver license example: {example_license}")
            if vehicles_ids:
                with conn.cursor() as cur:
                    cur.execute("SELECT plate_number FROM vehicles LIMIT 1")
                    example_plate = cur.fetchone()[0]
                    print(f"License plate example: {example_plate}")
            
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) as total_balances,
                        COALESCE(SUM(quantity), 0) as total_quantity
                    FROM stock_balances
                """)
                total_balances, total_qty = cur.fetchone()
                print(f"  - Stock balances: {total_balances} records, total quantity: {total_qty:,.0f}")
            
        except Exception as e:
            print(f"\nОШИБКА: {e}")
            import traceback
            traceback.print_exc()
        finally:
            if conn:
                conn.close()
                print("\nПодключение к базе закрыто.")