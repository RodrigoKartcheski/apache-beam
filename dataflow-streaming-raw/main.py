import json
import logging
import apache_beam as beam
from beam_nuggets.io import kafkaio
from beam import RawOptions, ToBQ, CheckJson
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

def main():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    pipe_options = pipeline_options.view_as(RawOptions)
    
    consumer_config = {
        "topic": "beev5",
        "bootstrap_servers": "34.138.147.93:9093"
    }

    dataset = pipe_options.dataset
    project = pipeline_options.display_data().get('project')
    path_to_raw_controller = pipe_options.path_to_raw_controller

    with beam.Pipeline(options=pipeline_options) as pipe:
        notifications = pipe | 'Lendo os dados do Kafka' >> kafkaio.KafkaConsume(
            consumer_config=consumer_config,
            value_decoder=bytes.decode
        )
        
        to_json = (
            notifications 
            | 'Transformando os dados em JSON' >> beam.Map(lambda doc: json.loads(doc[1])) 
            | 'Validando schema do JSON' >> beam.ParDo(CheckJson(project, path_to_raw_controller))
        )
        
        insert_bq = (
            to_json 
            | 'Inserindo dados no BQ' >> beam.ParDo(ToBQ(dataset, project, path_to_raw_controller))
        )
    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
