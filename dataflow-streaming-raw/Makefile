run:
	python3 main.py \
		--runner DataflowRunner \
		--project artigo-378815 \
		--temp_location gs://streaming-v1/temp \
		--job_name dataflow-streaming-v1 \
		--region us-east1 \
		--requirements_file ./requirements.txt \
		--setup_file ./setup.py \
		--experiments use_beam_bq_sink

run-dev:
	python3 main.py \
		--runner DirectRunner \
		--project focus-mechanic-321819 \
		--temp_location gs://beegdata_engineer/temp \
		--job_name dataflow-streaming-v1 \
		--region us-east1 \
		--requirements_file ./requirements.txt \
		--setup_file ./setup.py \
		--experiments use_beam_bq_sink \
		--dataset beegdata_homolog \
		--path_to_raw_controller gs://beegdata_engineer/metadados/raw_controller.csv