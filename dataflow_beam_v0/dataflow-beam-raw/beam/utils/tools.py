import ast
import pandas as pd
from google.cloud import bigquery

class Tools:

    def __init__(self, tabela: str, path_to_raw_controller: str):

        self.tabela = tabela

        df = pd.read_csv(path_to_raw_controller)

        df_json = df.to_dict('records')

        try:
            self.table_line_filtred = list(filter(lambda doc: (doc['source_table_name'] == self.tabela and doc['load_controller'] == 1), df_json))[0]
        except Exception as e:
            self.table_line_filtred = {}


    def get_schema_bq(self, simple_schema_str=False):
        fields_mapping = self.table_line_filtred.get('field_mapping')
        field_mapping = [(t[0], 'integer') if t[1] == 'int' or t[1] == 'bigint' else t for t in eval(fields_mapping)]

        if fields_mapping is not None:

            if simple_schema_str:
                return ', '.join([f"{t[0]}:{t[1].upper()}" for t in field_mapping])
            
            return [bigquery.SchemaField(t[0], t[1].upper()) for t in field_mapping]

        return None

    def get_patition_field(self):
        try:
            return self.table_line_filtred.get('partition_field')
        except Exception as e:
            return None
    
    def get_table_target(self):
        try:
            return self.table_line_filtred.get('target_table_name')
        except Exception as e:
            return None
    
    def create_table_bq(self, client, table_id):
        field_partition = self.table_line_filtred.get('partition_field')

        table = bigquery.Table(table_id, schema=self.get_schema_bq(simple_schema_str=False))
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=field_partition
        )
        table = client.create_table(table)
