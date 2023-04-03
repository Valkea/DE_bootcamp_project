#! /usr/bin/env python3

import re
import unidecode
import pathlib
import pandas as pd
from datetime import timedelta

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash


@task(retries=3)
def extract_from_gcs(dataset_file: str) -> pathlib.Path:
    """ Download trip data from GCS """

    gcs_path = pathlib.Path("data", dataset_file)
    localbase = "data2"
    localpath = pathlib.Path(localbase, gcs_path)

    gcs_block = GcsBucket.load("de-project-user-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=localbase)

    return localpath


@task(name="Transform Data for BQ", log_prints=True)
def transform_data(path: pathlib.Path) -> pd.DataFrame:
    """ Data cleaning example """

    df = pd.read_parquet(path)

    new_cols = {}
    for col in df.columns:
        new_col = unidecode.unidecode(col)
        new_col = re.sub(r'[^a-zA-Z0-9;]', '', new_col)
        new_cols[col] = new_col

    df.rename(columns=new_cols, inplace=True)

    return df


@task(retries=3)
def write_bq(df: pd.DataFrame, tablename: str) -> None:
    """ Write the DataFrame to Big Query Data Warehouse """
  
    gcp_credentials_block = GcpCredentials.load("de-project-gcs-creds")

    dataset_id = "de_project_staging"
    project_id = "compelling-moon-382321"
    tablename = tablename.replace('.parquet', '')

    df.to_gbq(
        destination_table=f"{dataset_id}.{tablename}",
        project_id=project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        # chunksize=500000,
        if_exists="append",
    )

@flow(name="", log_prints=True)
def etl_gcs_to_bq(dataset_file: str, tablename: str) -> None:
    """ Main ETL flow that pushes Data Lake data (GCS) Data Warhouse (Big Query) """

    path = extract_from_gcs(dataset_file)
    df = transform_data(path)
    write_bq(df, tablename)


@flow(log_prints=True)
def etl_gcs_to_bq_parent() -> None:
    """The base flow to loop over the ressources"""

    print("ETL - Gcs2Bq")
    with open(pathlib.Path("data-sources.txt"), "r") as f:
        for data_src in f.readlines():
            dataset_file, dataset_url = data_src.split(' ')
            dataset_file = dataset_file.replace('.csv', '.parquet')
            tablename, ext = dataset_file.split('.')
            etl_gcs_to_bq(dataset_file, tablename)


if __name__ == "__main__":
    etl_gcs_to_bq_parent()
