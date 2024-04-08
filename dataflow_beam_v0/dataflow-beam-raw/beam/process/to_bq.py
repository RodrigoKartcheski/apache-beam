import logging
from apache_beam import DoFn
from beam.utils import Tools
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

class ToBQ(DoFn):

    def __init__(self, dataset, project, path_to_raw_controller):
        self.dataset = dataset.get()
        self.project = project
        self.path_to_raw_controller = path_to_raw_controller.get()

    def process(self, doc):
        try:
            tools = Tools(
                tabela=doc.get('tabela'), 
                path_to_raw_controller=self.path_to_raw_controller
            )
            
            client = bigquery.Client()

            table_ref = client.dataset(self.dataset, project=self.project).table(tools.get_table_target())

            table_id = f"{self.project}.{self.dataset}.{tools.get_table_target()}"

            try:
                table_check = client.get_table(table=table_ref)
                print("Table {} already exists.".format(table_id))

                if not table_check:
                    try:
                        tools.create_table_bq(client=client, table_id=table_id)
                    except Exception as e:
                        logging.error(f'Ocorreu um erro ao criar a tabela {table_id}: {e}')
                        return
                
            except NotFound:
                print("Table {} is not found.".format(table_id))
                try:
                    tools.create_table_bq(client=client, table_id=table_id)
                except Exception as e:
                    logging.error(f'Ocorreu um erro ao criar a tabela {table_id}: {e}')
                    return
            
            doc.pop('tabela')
                    
            rows_to_insert = [doc]

            table = client.get_table(table=table_ref)

            client.insert_rows(
                table,
                rows_to_insert,
            )

            yield
        except Exception as e:
            logging.info(str(e) + 'Exception TOBQ')
            yield
