import pandas as pd
import os

SILVER_PATH = "data/silver"


def main():
    telemetry = pd.read_parquet(f"{SILVER_PATH}/json/telemetry.parquet")
    print(telemetry[['order_key', 'timestamp']][telemetry['order_key']=='230'])   
main()