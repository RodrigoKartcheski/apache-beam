import logging
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

import re
from apache_beam import DoFn
from google.cloud import bigquery

class ReadController(DoFn):

    def __init__(self, path_controller: str):
        self.path_controller = path_controller

    def process(self, element):
        df = pd.read_csv(self.path_controller)

        df_json = df.to_dict('records')

        for doc in df_json:
            if doc.get('flag_controller') == 1:
                yield doc

class MergeToRefined(DoFn):

    def process(self, doc_controller: dict):
        with open(f'./{doc_controller.get("file_path_query")}', 'r') as file_sql:
            query_sql = file_sql.read()
        
        print("\nIniciando carga na tabela ", f"{doc_controller.get('destination_table')}")
        query_sql = re.sub(r'\{dataset_refined\}', f"{doc_controller.get('dataset_refined')}", query_sql)
        query_sql = re.sub(r'\{destination_table\}', f"{doc_controller.get('destination_table')}", query_sql)
        query_sql = re.sub(r'\{dataset_trusted\}', f"{doc_controller.get('dataset_trusted')}", query_sql)
        #query_sql = re.sub(r'\{interval_time\}', f"{doc_controller.get('interval_time_hour')}", query_sql)
        query_sql = re.sub(r'\{dataset_trusted\}', f"{doc_controller.get('columns_merge')}", query_sql)
        query_sql = re.sub(r'\{interval_time\}', "5000", query_sql)
        
        
        client = bigquery.Client()
        job = client.query(query_sql)
        job.result()

        if job.errors:
            print(job.errors)
            logging.error("Erro durante a execução da consulta: {}".format(job.errors))
            
def converter_string(string):
    fields = string.split('|')
    conditions = []

    for field in fields:
        condition = f"T.{field} = S.{field}"
        conditions.append(condition)

    result = " AND ".join(conditions)
    return result


string = "user_id|message|created_at|title|status|score|firstaccess|lastaccess|database|dateload|op"

resultado = converter_string(string)
print(resultado)

def main():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    # pipe_options = pipeline_options.view_as()

    project = pipeline_options.display_data().get('project')
    path_to_raw_controller = 'gs://beegdata_engineer/metadados/refined_controller.csv'

    with beam.Pipeline(options=pipeline_options) as pipe:
        table_controller = (
            pipe |
            beam.Create(['START']) |
            beam.ParDo(ReadController(path_to_raw_controller))
        )

        merge_refined = (
            table_controller |
            beam.ParDo(MergeToRefined())
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()


