1. Установите docker 
2. скопируйте репозиторий при помощи git clone
3. Перейдите в корневую папку проекта: ```cd diploma-star```
4. Дайте доступ к докеру локальному пользователю: ```sudo usermod -aG $USER```
5. Перезагрузите ПК
6. Введите docker compose up -d
7. Зайдите в папку проекта и создатей виртуальную среду: ```python -m venv .venv```
8. Активируйте виртуальную среду:\
    Для Linux: ```source .venv/bin/activate``` 
    Для Windows: ``````
    Для MacOS: ``````
9. Установите необходимые библиотеки: ```pip install -r requirements.txt```
10. Создайте в корневой папке проекта файл .env и заполните его:
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
11. Откройте postgresql и создайте базу данных: ```CREATE DATABASE shop_db;``` вы можете использовать своё название базы данных, но не забудьте отразить это в файле .env
12. Отредактируйте файл ```pg_hba.conf``` Добавьте в конец файла строку:\
    ```host    all             all             172.18.0.0/16           md5``` и

    Отредактируйте файл ```postgresql.conf``` Раскомментируйте строку, начинающуюся с ```listen_addresses``` и измените , так чтобы она выглядела ```listen_addresses = '*'```
13. Запустите файл генерации данных для работы: ```data_generator/generation.py```
14. В окне браузера откройте ```http://localhost:8780```
15. Перезапустите постгрес ```sudo systemctl restart postgresql```
16. В веб-браузере перейдите по адресу ```http://localhost:8780``` Авторизуйтесь (По умолчанию Логин:```airflow``` Пароль:```airflow```)
17. Создайте подключения. Для этого перейдите во вкладку Admin > Connections И создайте 3 подключения:
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
18. Перейдите во вкладку DAGs и запустите bronze_load
19. Заупстите silver_layer_etl
20. Создайте файл в папке ```etc/clickhouse-server/config.d/allow_all_hosts.xml```
    Содержание:
    ```
        <?xml version="1.0"?>
        <yandex>
            <listen_host>0.0.0.0</listen_host>
        </yandex>
    ```
    Перезапустите clickhouse
21. Через терминал создайте базу данных в clickhouse:\
```clickhouse-client --query "CREATE DATABASE IF NOT EXISTS gold" --password <ваш_пароль>```

    Запустите скрипт создания структуры БД для золотого слоя:\
```clickhouse-client --database gold < create_database_gold.sql --password <ваш_пароль>```
22. В airflow запустите gold_layer clickhouse
