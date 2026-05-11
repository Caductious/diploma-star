# Разработка хранилища на основе денормализованных данных по схеме "Звезда"
Выпускная квалификационная работа. Проект представляет из себя генерацию 3 источников данных(xlsx таблицы, JSON и операционная база данных на PostgreSQL), ETL процесс реализованный на python при помощи Apache airflow с сохранением данных в формате parquet и последующей загрузкой в Clickhouse.\

**Научный руководитель**: Кандидат технических наук Журавлева Марина Гарриевна\
**Выполнил**: Студент группы ИД22-4 Логинов Пётр Константинович\
**Год**: 2026\
**Направление подготовки**: 09.03.03 прикладная информатик
**Программа подготовки**: Инженерия данных

### Технические требования
PostgreSQL 16\
Python 3.11+\
Clickhouse 26.3.9+\
Apache Airflow\
Docker

## Инструкция по установке
1. скопируйте репозиторий при помощи git clone
2. Перейдите в корневую папку проекта: ```cd diploma-star```
3. Дайте доступ к докеру локальному пользователю: ```sudo usermod -aG $USER```
4. Перезагрузите ПК
5. Введите docker compose up -d
6. Зайдите в папку проекта и создатей виртуальную среду: ```python -m venv .venv```
7. Активируйте виртуальную среду:\
    Для Linux: ```source .vROADMAP.md — полная архитектура, формулы, FAQ для защиты
env/bin/activate``` 
    Для Windows: ``````
    Для MacOS: ``````
8. Установите необходимые библиотеки: ```pip install -r requirements.txt```
9. Создайте в корневой папке проекта файл .env и заполните его:
    ```
    AIRFLOW_UID=1000

    PGDBNAME=shop_db
    PGUSER=<имя_пользователя>
    PGPASSWORD=<пароль>
    HOST=localhost
    PORT=5432

    CLICKHOUSEDBNAME=gold
    CLICKHOUSEUSER=<имя_пользователя>
    CLICKHOUSEPASSWORD=<пароль>
    CLICKHOUSEHOST=172.17.0.1
    CLICKHOUSEPORT=9000
    ```
10. Откройте postgresql и создайте базу данных: ```CREATE DATABASE shop_db;``` вы можете использовать своё название базы данных, но не забудьте отразить это в файле .env
11. Отредактируйте файл ```pg_hba.conf``` Добавьте в конец файла строку:\
    ```host    all             all             172.18.0.0/16           md5``` и
12. Отредактируйте файл ```postgresql.conf``` Раскомментируйте строку, начинающуюся с ```listen_addresses``` и измените , так чтобы она выглядела ```listen_addresses = '*'``` 
13. Перезапустите постгрес ```sudo systemctl restart postgresql```
14. Запустите файл генерации данных для работы: ```data_generator/generation.py```
15. В веб-браузере перейдите по адресу ```http://localhost:8780``` Авторизуйтесь (По умолчанию Логин:```airflow``` Пароль:```airflow```)
16. Создайте подключения. Для этого перейдите во вкладку Admin > Connections И создайте 3 подключения:
    1. Для постгрес:
    ```
    Connection Id: postgres_diploma
    Connection Type: Postgres
    Host: 172.17.0.1
    Database: shop_db
    Login: (ваш логин)
    Password: (ваш пароль)
    Port: 5432
    ```
    2. Для файловой системы:
    ```
    Connection Id: fs_default
    Connection Type: File (path)
    Path: {"path": "/opt/airflow/data/raw"}
    ```
    3. Для кликхаус:
    ```
    Connection Id: clickhouse_default
    Connection Type: MySQL
    Host: host.docker.internal
    Schema: gold
    Login: (default)
    Port: 9000
    Extra: {
            "database": "gold",
            "host": "host.docker.internal"
            }
    ```
17. Перейдите во вкладку DAGs и запустите bronze_load
18. Заупстите silver_layer_etl
19. Создайте файл в папке ```etc/clickhouse-server/config.d/allow_all_hosts.xml```
    Содержание:
    ```
    <?xml version="1.0"?>
    <yandex>
        <listen_host>0.0.0.0</listen_host>
    </yandex>
    ```
    Перезапустите clickhouse ```sudo systemctl restart clickhouse```
20. Через терминал создайте базу данных в clickhouse:\
```clickhouse-client --query "CREATE DATABASE IF NOT EXISTS gold" --password <ваш_пароль>```

    Запустите скрипт создания структуры БД для золотого слоя:\
```clickhouse-client --database gold < create_database_gold.sql --password <ваш_пароль>```
21. В airflow запустите gold_layer clickhouse
