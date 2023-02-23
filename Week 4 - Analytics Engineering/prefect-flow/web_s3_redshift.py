from pathlib import Path
import pandas as pd
import numpy as np
import math
from prefect import flow, task
from random import randint
from prefect_aws.s3 import S3Bucket
from prefect.tasks import task_input_hash
from time import time
from datetime import timedelta
from prefect_aws import AwsCredentials
import redshift_connector
from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import create_engine



@task(retries=3,  cache_key_fn=task_input_hash, cache_expiration=timedelta(days=10))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtypes issues"""

    if "tpep_pickup_datetime" in df.columns:
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    if "lpep_pickup_datetime" in df.columns:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    return df

@task()
def write_s3(path: Path) -> None:
    """Uploading local Parquet file to s3 bucket"""
    
    parentPath = Path(__file__).parent

    savepath = (Path(parentPath, path))
    s3_block = S3Bucket.load("zoom-s3-block")

    s3_block.upload_from_path(
        from_path=savepath, 
        to_path=path
    )

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_s3(color: str, year: int, month: int) -> Path:
    "Download trip from rs3 bucket"
    s3_block = S3Bucket.load("zoom-s3-block")

    s3_path = f"data/{color}"
    dataset_file = f"{color}_tripdata_{year}-{month:02}.parquet"

    localpath = Path(Path(__file__).parents[1], s3_path)
    localpath.mkdir(parents = True, exist_ok = True)
    s3_block.download_object_to_path(from_path=s3_path + "/" + dataset_file, to_path=Path(localpath, dataset_file))

    return localpath / dataset_file

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=10))
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame locally as parquet file"""

    parentPath = Path(__file__).parent
    path = Path(f"data/{color}")

    savepath = (Path(parentPath, path))
    savepath.mkdir(parents = True, exist_ok = True)     
    file = f"{dataset_file}.parquet"
    df.to_parquet(Path(savepath, file), compression="gzip")

    return Path(savepath, file)    

@flow()
def transform(path: Path) -> pd.DataFrame:
    """Data Cleaning example"""

    df = pd.read_parquet(path)
    return df


def split_dataframe(df, chunk_size = 10000): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    for chunck in chunks:
        yield chunck    

@task()
def write_redshift(df: pd.DataFrame) -> None:
    """Write DataFrame to redshift"""

    connection_block = SqlAlchemyConnector.load("redshift-block")
    print(f"Number of iteration is {math.ceil(len(df)/10000)}")
    df_iter = split_dataframe(df, 100000)
 
    while True:
        try:
            dff = next(df_iter)

            if "tpep_pickup_datetime" in dff.columns:
                with connection_block.get_connection(begin=False) as engine:
                    dff.to_sql(name="yellow_trip_data", con=engine, index=False, if_exists="append", method="multi", schema="trips_data_all")

            if "lpep_pickup_datetime" in dff.columns:
                with connection_block.get_connection(begin=False) as engine:
                    dff.to_sql(name="green_trip_data", con=engine, index=False, if_exists="append", method="multi", schema="trips_data_all")
        except StopIteration:
            break

    print("Finished loading set")

@flow()
def etl_s3_to_redshift(year: int, month: int, color: str):
    """Main ETL to load data into amazon redshift"""

    path = extract_from_s3(color, year, month)
    df = transform(path)
    write_redshift(df)

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1,2], year: int = 2021, color: str = "yellow"
):

    for month in months:

        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"     

        df = fetch(dataset_url)
        df_clean = clean(df)
        path = write_local(df_clean, color, dataset_file)
        #write_s3(path)    
        #path = extract_from_s3(color, year, month)
        df = transform(path)
        write_redshift(df)


if __name__ == "__main__":

    color = "green"
    months = [i for i in range(1, 13)]
    year = 2019
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")    

    color = "yellow"
    months = [i for i in range(1, 13)]
    year = 2019
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")

    color = "yellow"
    months = [i for i in range(1, 13)]
    year = 2020
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")

    color = "green"
    months = [i for i in range(1, 13)]
    year = 2019
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")

    color = "green"
    months = [i for i in range(1, 13)]
    year = 2020
    etl_parent_flow(months, year, color)
    print(f"Finished 'etl_parent_flow' for {color} + {year}")