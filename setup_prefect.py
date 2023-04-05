import argparse
from prefect.blocks.system import JSON
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
# from prefect_gcp.bigquery import BigQueryWarehouse


def register_blocks(cred_path, project_id):

    # --- Register some variables

    json_block = JSON(value={"project_id": project_id})
    json_block.save(name="eco2mix-de-project-variables", overwrite=True)

    # --- Register GCP credential block

    credentials_block = GcpCredentials(service_account_file=cred_path)
    credentials_block.save("eco2mix-de-project-creds", overwrite=True)

    # --- Register GCP bucket storage block

    bucket_block = GcsBucket(
        gcp_credentials=GcpCredentials.load("eco2mix-de-project-creds"),
        bucket="eco2mix-de-project-bucket",
    )
    bucket_block.save("eco2mix-de-project-bucket", overwrite=True)

    # --- Register GCP BigQuery block

    # bq_block = BigQueryWarehouse(
    #     gcp_credentials=GcpCredentials.load("eco2mix-de-project-creds"),
    #     fetch_size=1
    # )
    # bq_block.save("eco2mix-de-project-bigquery", overwrite=True)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('cred_path', type=str, help="The path to the GCP credential JSON")
    parser.add_argument('project_id', type=str, help="The GCP project id")
    args = parser.parse_args()

    register_blocks(args.cred_path, args.project_id)

    j = JSON.load("eco2mix-de-project-variables")
    print("TEST:", j)
    print("TEST:", j.value['project_id'])
