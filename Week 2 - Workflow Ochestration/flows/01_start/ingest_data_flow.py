import numpy as np
import pandas as pd
import argparse
from sqlalchemy import create_engine
import os
from prefect import flow, task
from prefect.tasks import task_input_hash
from time import time
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    #download the csv
    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    return df

@task(log_prints=True)
def transform_data(df):
    print(f"Pre: missing passenger_count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] !=0]
    print(f"Post: missing passenger_count: {df['passenger_count'].isin([0]).sum()}")

    return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("posgres-block")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name="zones", con=engine, if_exists="replace")
        df.to_sql(name=table_name, con=engine, if_exists="append")

@flow(name="Subflow, log_prints=True")
def log_subflow(table_name: str):
    print(f"Logging subflow for {table_name}")


@flow(name="Ingest Flow")        
def main_flow(table_name: str):
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(url)
    data = transform_data(raw_data)
    ingest_data(table_name, data)

if __name__ == "__main__":
    main_flow("yellow_taxi_data")
