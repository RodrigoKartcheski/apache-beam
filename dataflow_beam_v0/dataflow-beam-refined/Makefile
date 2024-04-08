run-prod:
	python3 main.py \
		--runner DataflowRunner \
		--project focus-mechanic-321819 \
		--temp_location gs://beegdata_engineer/temp \
		--job_name dataflow-streaming-v2 \
		--region us-east4 \
		--requirements_file ./requirements.txt \
		--setup_file ./setup.py \
		--dataset beegdata_dev_raw \
		--path_to_raw_controller gs://beegdata_engineer/metadados/raw_controller.csv

run-dev:
	python3 main.py \
		--runner DirectRunner \
		--project focus-mechanic-321819 \
		--temp_location gs://beegdata_engineer/temp \
		--job_name dataflow-streaming-refined \
		--region us-east4

run-deploy:
	python3 main.py \
		--runner DataflowRunner \
		--project focus-mechanic-321819 \
		--temp_location gs://beegdata_engineer/temp \
		--job_name dataflow-streaming-v2 \
		--region us-east4 \
		--requirements_file ./requirements.txt \
		--setup_file ./setup.py \
		--dataset beegdata_dev_raw \
		--path_to_raw_controller gs://beegdata_engineer/metadados/raw_controller.csv \
		--template_location gs://beegdata_engineer/templates/raw_streaming_template