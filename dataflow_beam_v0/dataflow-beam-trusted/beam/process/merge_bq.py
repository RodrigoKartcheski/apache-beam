import logging
from apache_beam import DoFn
from google.cloud import bigquery
from beam.utils.tools import Tools

class MergeBQ(DoFn):
    def __init__(self, project, path_controller):
        self.tools = Tools(project, path_controller)

    def process(self, doc: dict):
        try:
            # doc = self.tools.extract_emojis(doc, doc.get('source_table'))
            # doc = self.tools.extract_mentions(doc, doc.get('source_table'))

            client = bigquery.Client()

            self.tools.check_table(client, doc.get('source_table'), field_partition=doc.get('field_partition'), create_table=True)

            pk_values = [int(doc.get(pk)) if isinstance(doc.get(pk), int) else f"'{doc.get(pk)}'" for pk in doc.get('primary_keys')]

            pk_string = " AND ".join([f"{pk}={pk_values[i]}" for i, pk in enumerate(doc.get('primary_keys'))])

            columns_names = doc.get('columns_names')
            update_columns_list = [f"{column}={doc[value]!r}" if value in doc and doc[value] is not None else f"{column}=NULL" for column, value in columns_names.items()]
            update_string = ", ".join(update_columns_list)

            update_statement = f"UPDATE `{doc.get('table_id')}` SET {update_string} WHERE {pk_string}"

            query_job = client.query(f"SELECT {', '.join(doc.get('primary_keys'))} FROM `{doc.get('table_id')}` WHERE {pk_string}")
            results = query_job.result()

            if len(list(results)) > 0:

                if doc.get('Op') == 'DELETE':
                    delete_statement = f"DELETE FROM `{doc.get('table_id')}` WHERE {pk_string};"
                    client.query(delete_statement)
                else:
                    print(update_statement)
                    client.query(update_statement)
                    
            else:
                if doc.get('Op') == 'INSERT':
                    insert_columns = [f"{doc[column]!r}" if column in doc and doc[column] is not None else f"NULL" for column, value in columns_names.items()]
                    insert_string = ", ".join(insert_columns)
                    insert_statement = f"INSERT INTO {doc.get('table_id')} VALUES ({insert_string})"

                    print(insert_statement)

                    print(f'INSERT', insert_statement)
                    client.query(insert_statement)

        except Exception as e:
            logging.error(str(e) + 'Merge BQ.')
        yield
       