from pathlib import Path
import pandas as pd
from prefect import flow, task
from random import randint
from prefect_aws.s3 import S3Bucket
from prefect.tasks import task_input_hash
from time import time
from datetime import timedelta
from sqlalchemy import text
from subprocess import check_output

@task(retries=3,  cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix dtypes issues"""

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame locally as parquet file"""

    parentPath = Path(__file__).parent
    path = Path(f"data/{color}")

    savepath = (Path(parentPath, path))
    savepath.mkdir(parents = True, exist_ok = True)     
    file = f"{dataset_file}.parquet"
    df.to_parquet(Path(savepath, file), compression="gzip")

    return Path(path, file)

@task()
def write_s3(path: Path) -> None:
    """Uploading local Parquet file to s3 bucket"""

    s3_block = S3Bucket.load("zoom-s3-block")

    s3_block.upload_from_path(
        from_path=path, 
        to_path=path
    )


@flow()
def etl_web_to_s3() -> None:
    """The main ETL function"""

    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_s3(path)    

if __name__ == "__main__":
    etl_web_to_s3()