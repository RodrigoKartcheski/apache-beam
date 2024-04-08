from apache_beam import DoFn
from beam.utils import Tools
import logging

class CheckJson(DoFn):

    def __init__(self, project, path_to_raw_controller):
        self.project = project
        self.path_to_raw_controller = path_to_raw_controller.get()

    def process(self, doc: dict):
        try:
            doc_metadata = doc.get('metadata')

            tools = Tools(
                tabela=doc_metadata.get('table-name'), 
                path_to_raw_controller=self.path_to_raw_controller
            )

            mapping = tools.get_schema_bq(simple_schema_str=True)
            
            columns_list = [x.split(':')[0].strip() for x in mapping.split(',')]

            doc_new = {k: str(v) if k != 'cdc_commit_timestamp' else v for k, v in doc.get('data').items() if k in columns_list}

            doc_new['tabela'] = doc_metadata.get('table-name')
            doc_new['database'] = doc_metadata.get('schema-name')
            doc_new['dateload'] = doc_metadata.get('timestamp')

            yield doc_new
        except Exception as e:
            logging.info(str(e) + 'Exception CheckJson')
            yield
        