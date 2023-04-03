#! /usr/bin/env python3

from prefect import flow
from etl_web_to_gcs import etl_web_to_gcs_parent
from etl_gcs_to_bq import etl_gcs_to_bq_parent


@flow(log_prints=True)
def etl_main_flow() -> None:
    """
    The base flow that sequentially calls the other scripts / flows.

    (I could merge both script into one single script, but I prefer
    to keep them separated as an example of the possibilites for
    incoming projects...)
    """

    print("Call Web2GCS")
    etl_web_to_gcs_parent()
    print("Call GCS2BQ")
    etl_gcs_to_bq_parent()


if __name__ == "__main__":
    etl_main_flow()
