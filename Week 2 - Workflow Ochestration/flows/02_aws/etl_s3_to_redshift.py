from pathlib import Path
import pandas as pd
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

@flow()
def transform(path: Path) -> pd.DataFrame:
    """Data Cleaning example"""
    
    df = pd.read_parquet(path)
    print(f"Pre: missing passenger_count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] !=0]
    print(f"Post: missing passenger_count: {df['passenger_count'].isin([0]).sum()}")

    return df

@task()
def write_redshift(df: pd.DataFrame) -> None:
    """Write DataFrame to redshift"""

    print(f"Length is {len(df)}")
    connection_block = SqlAlchemyConnector.load("redshift-block")
    with connection_block.get_connection(begin=False) as engine:
        df.head(100000).to_sql(name="dezoomcamp.rides", con=engine, index=False, if_exists="replace", method="multi")

    print("Uploaded to redshift")


@flow()
def etl_s3_to_redshift():
    """Main ETL to load data into amazon redshift"""

    color = "yellow"
    year = "2021"
    month = 1

    path = extract_from_s3(color, year, month)
    df = transform(path)
    write_redshift(df)

if __name__ == "__main__":
    etl_s3_to_redshift()