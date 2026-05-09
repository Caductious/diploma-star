import pandas as pd
from pathlib import Path

# Укажите реальный путь к Excel
excel_path = "data/raw/excel/август_2025.xlsx"  # замените на ваш

df = pd.read_excel(excel_path)
print("DataFrame прочитан")
print(df.head())
print(df.dtypes)

# Пробуем сохранить в Parquet
df.to_parquet("test.parquet")

# Пробуем прочитать
df2 = pd.read_parquet("test.parquet")
print("Успешно!")
print(df2)