# DE_bootcamp_project

## Data

- https://odre.opendatasoft.com/explore/dataset/temperature-quotidienne-regionale
- https://odre.opendatasoft.com/explore/dataset/stock-quotidien-stockages-gaz/
- https://odre.opendatasoft.com/explore/dataset/eco2mix-national-tr

## Install PREFECT and other libs

>>> python -m venv venvProject
>>> source venvProject/bin/activate
>>> pip install -r requirements.txt

>>> prefect version

## Run local PREFECT GUI

>>> prefect orion start
open http://127.0.0.1:4200/



## --------------------------------------------------------------
## Do ETL and then send the resulting file to Google Cloud Storage (gcs)

we write a whole new script (see 02_gcp)

### Let's create a bucket to store the project files

- Go to GCP-UI // "Cloud Storage" / "Buckets" / "Create"

name --> de_project_gcp_bucket
data location type --> europe-west1(Belgium)
data storage class --> standard (Best for short-term storage and frequently accessed data)

- Click on "CREATE"

### We can see the bucket content

- Go to GCP-UI // "Cloud Storage" / "Buckets" / "de_project_gcp_bucket"

### Let's create an account with the appropriate rights 

- Go to GCD-UI // "IAM & Admin" / "Service Account" / "Create service account"

-->1/ service account-name: PROJECT-NAME-user-bucket (de_project_user_bucket)
-->2/ role: "BigQuery Admin" & "Cloud Storage / Storage Admin"
--> Save
-->3/ .
--> Done

### Let's create a remote key for the account

- Go to GCD-UI // "IAM & Admin" / "Service Account"
- Click '...' and "Manage keys" on the appropriate service account (de_project_user_bucket)
- Go to "Add Key" / "Create new key" / "JSON" --> A json file is saved to the computer

### Let's add the PREFECT GCP Block

Register blocks types within a module or file.
                                                                                                                                                                                   
#### Make sure the targeted blocks is available for configuration via the UI. (If a block type has already been registered, its registration will be updated to match the block's current definition)

>>> prefect block register -m prefect_gcp 

#### PREFECT-GUI ## "Blocks" / "Add Block" / "GCS Bucket"

name --> de-project-user-bucket
bucket-name --> de_project_gcp_bucket

#### Click "ADD" on GCP-Credentials

name --> de-project-gcs-creds
service account info --> copy the content of the JSON file associated with the bucket account (we downloaded it earlier)

--> Done
--> Select the newly created  GCP credential in the select box
--> Create


#### Edit script
==> Add GcsBucket (as explained on the PREFECT-GUI GcSBucket block page)



