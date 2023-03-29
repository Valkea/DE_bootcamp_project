#! /usr/bin/env python3

import re
import unidecode
import os
import pathlib
import pandas as pd
from datetime import timedelta

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash


@task(
    name="Extract Data",
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame"""

    print("FETCH", dataset_url)
    return pd.read_csv(dataset_url, delimiter=";")


@task(name="Transform Data", log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:

    print(df.head(2))

    new_cols = {}
    for col in df.columns:
        new_col = unidecode.unidecode(col)
        new_col = re.sub(r'[^a-zA-Z0-9;]', '', new_col)
        new_cols[col] = new_col

    df.rename(columns=new_cols, inplace=True)

    # print(f"pre: columns types: \n{df.dtypes}")
    # if "lpep_pickup_datetime" in df.columns:
    #     df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    #     df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    # elif "tpep_pickup_datetime" in df.columns:
    #     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    # print(f"post: columns types: \n{df.dtypes}")

    # print(f"pre: missing passenger count: {df[df.passenger_count == 0].shape[0]} / {df.shape[0]}")
    # df = df[df.passenger_count != 0]
    # print(f"post: missing passenger count: {df[df.passenger_count == 0].shape[0]} / {df.shape[0]}")

    return df


@task(name="Write Data Locally", log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> pathlib.Path:
    """Write the DataFrame out locally as a Parquet file"""

    path = pathlib.Path("data")
    if not path.exists():
        os.makedirs(path)

    path1 = pathlib.Path(path, dataset_file)
    df.to_csv(path1, index=False)

    path = pathlib.Path(path, f"{dataset_file.replace('.csv','.parquet')}")
    df.to_parquet(path, compression="gzip", index=False)

    return path


@task(name="Write Data on GCS", log_prints=True)
def write_GCS(path: pathlib.Path) -> None:
    """Copy the Parquet file to GCS"""

    gcs_block = GcsBucket.load("de-project-user-bucket")
    gcs_block.upload_from_path(from_path=path, to_path=path)


@flow(log_prints=True, retries=3)
def etl_web_to_gcs(dataset_url, dataset_file) -> None:
    """The main ETL function"""

    # dataset_file = f"{color}_tripdata_{year}-{month:02}.csv.gz"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}"

    df = fetch(dataset_url)
    df_clean = transform_data(df)
    path = write_local(df_clean, dataset_file)
    write_GCS(path)


@flow(log_prints=True)
def etl_parent_flow() -> None:

    """The main function"""
    with open(pathlib.Path("data-sources.txt"), "r") as f:
        for data_src in f.readlines():
            dataset_file, dataset_url = data_src.split(' ')
            print("ETL for :", dataset_file, dataset_url)
            etl_web_to_gcs(dataset_url, dataset_file)


if __name__ == "__main__":
    etl_parent_flow()
