run-dev:
	python3 main.py \
		--runner DirectRunner \
		--project focus-mechanic-321819 \
		--temp_location gs://beegdata_engineer/temp \
		--job_name dataflow-streaming-v2 \
		--region us-east4 \
		--setup_file ./setup.py \
		--requirements_file ./requirements.txt \
		--path_controller gs://beegdata_engineer/metadados/trusted_controller.csv

run-prod:
	python3 main.py \
		--runner DataflowRunner \
		--project focus-mechanic-321819 \
		--temp_location gs://beegdata_engineer/temp \
		--job_name dataflow-streaming-v2 \
		--region us-east4 \
		--setup_file ./setup.py \
		--requirements_file ./requirements.txt \
		--path_controller gs://beegdata_engineer/metadados/trusted_controller.csv \
		--template_location gs://beegdata_engineer/templates/trusted_streaming_template