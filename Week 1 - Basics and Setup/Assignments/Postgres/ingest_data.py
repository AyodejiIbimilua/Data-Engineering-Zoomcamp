import numpy as np
import pandas as pd
import argparse
from sqlalchemy import create_engine
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    #download the csv

    os.system(f"wget {url} -O {csv_name}")


    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists='append')
    
    while True:
        try:
            df = next(df_iter)

            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

            df.to_sql(name=table_name, con=engine, if_exists="append")
        except StopIteration:
            break

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")


    parser.add_argument('--user', help='username for postgress')
    parser.add_argument('--password', help='password for postgress')
    parser.add_argument('--host', help='host for postgress')
    parser.add_argument('--port', help='port for postgress')
    parser.add_argument('--db', help='database name for postgress')
    parser.add_argument('--table_name', help='name of table postgress')
    parser.add_argument('--url', help='url for csv')

    args = parser.parse_args()

    main(args)
#print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))