from apache_beam import DoFn
from beam.utils import Tools

class CheckJson(DoFn):

    def __init__(self, project, path_to_raw_controller):
        self.project = project
        self.path_to_raw_controller = path_to_raw_controller.get()

    def process(self, doc: dict):
        tools = Tools(
            tabela=doc.get('table'), 
            path_to_raw_controller=self.path_to_raw_controller
        )

        mapping = tools.get_schema_bq(simple_schema_str=True)
        
        columns_list = [x.split(':')[0].strip() for x in mapping.split(',')]

        doc_new = {k: v for k, v in doc.get('data').items() if k in columns_list}

        doc_new['tabela'] = doc.get('table')
        doc_new['database'] = doc.get('database')

        yield doc_new