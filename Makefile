
PROJECT_ID = "compelling-moon-382321"
CREDS_PATH = "creds/compelling-moon-382321-1a9730cde10d.json"

setup_prefect_blocks:
	@echo "Initialiaze prefect blocks"
	python setup_prefect.py ${CREDS_PATH} ${PROJECT_ID}

setup_prefect_deployment:
	@echo "Initialize prefect deployment"
	prefect deployment build flows/etl_main.py:etl_main_flow --name etl_eco2mix --cron "0 * * * *" -a

start_agent:
	@echo "Start prefect agent"
	prefect agent start --work-queue "default"

setup: setup_prefect_blocks setup_prefect_deployment
