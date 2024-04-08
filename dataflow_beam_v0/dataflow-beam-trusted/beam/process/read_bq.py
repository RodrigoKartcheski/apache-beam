import logging
from apache_beam import DoFn
from beam.utils.tools import Tools
from google.cloud import bigquery

class ReadBQ(DoFn):

    def __init__(self, project, path_controller):
        self.tools = Tools(project, path_controller)

    def process(self, table):
        try:
            client = bigquery.Client()
            print(table)
            check_table = self.tools.check_table(client, table)

            carga_full = False
            if not check_table:
                carga_full = True

            query = self.tools.query_sql(table, carga_full)

            logging.info(query)
            
            rows = client.query(query).result()

            for row in rows:
                yield self.tools.set_schema_json(list(row), table)
                
        except Exception as e:
            logging.error(str(e) + ' Lendo tabela BQ.')
            yield {}
