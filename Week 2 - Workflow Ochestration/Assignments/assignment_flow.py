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
    return df

@task()
def write_redshift(df: pd.DataFrame) -> int:
    """Write DataFrame to redshift"""

    print(f"Length is {len(df)}")
    connection_block = SqlAlchemyConnector.load("redshift-block")
    with connection_block.get_connection(begin=False) as engine:
        df.head(50).to_sql(name="assignment.rides", con=engine, index=False, if_exists="append", method="multi")

    print("Uploaded to redshift")

    return len(df)


@flow()
def etl_s3_to_redshift(year: int, month: int, color: str):
    """Main ETL to load data into amazon redshift"""

    path = extract_from_s3(color, year, month)
    df = transform(path)
    write_redshift(df)

@flow(log_prints=True)
def etl_assignment_flow(
    months: list[int] = [1,2], year: int = 2021, color: str = "yellow"
):
    nrows = []
    for month in months:
        ldf = etl_s3_to_redshift(year, month, color)

        nrows.append(len(ldf))
    
    print(sum(nrows))

if __name__ == "__main__":
    color = "yellow"
    months = [2,3]
    year = 2019
    etl_assignment_flow(months, year, color)