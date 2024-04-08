import json
import logging
import apache_beam as beam
from beam_nuggets.io import kafkaio
from beam import RawOptions, ToBQ, CheckJson, KafkaC
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

def main():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    pipe_options = pipeline_options.view_as(RawOptions)

    dataset = pipe_options.dataset
    project = pipeline_options.display_data().get('project')
    path_to_raw_controller = pipe_options.path_to_raw_controller

    with beam.Pipeline(options=pipeline_options) as pipe:
        
        for id_task in range(1):
            consumer_1 = (
                pipe | 
                f'[TASK-ID-{id_task}-1] Lendo os dados do Kafka' >> KafkaC(
                    bootstrap_servers=['100.26.150.11:9093'],
                    topic='meu_time'
                ) | 
                f'[TASK-ID-{id_task}-1] Transformando os dados em JSON' >> beam.Map(lambda doc: json.loads(doc[1]))
            )

            consumer_2 = (
                pipe | 
                f'[TASK-ID-{id_task}-2] Lendo os dados do Kafka' >> KafkaC(
                    bootstrap_servers=['100.26.150.11:9093'],
                    topic='meu_time'
                ) | 
                f'[TASK-ID-{id_task}-2] Transformando os dados em JSON' >> beam.Map(lambda doc: json.loads(doc[1]))
            )

            list_consumers = [consumer_1, consumer_2]
            id_task_consumer = 0
            
            for consumer in list_consumers:
                to_json = (
                    consumer |
                    f'[TASK-ID-{id_task}-{id_task_consumer}] Validando schema do JSON' >> beam.ParDo(CheckJson(project, path_to_raw_controller))
                )

                insert_bq = (
                    to_json 
                    | f'[TASK-ID-{id_task}-{id_task_consumer}] Inserindo dados no BQ' >> beam.ParDo(ToBQ(dataset, project, path_to_raw_controller))
                )
                id_task_consumer += 1

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
