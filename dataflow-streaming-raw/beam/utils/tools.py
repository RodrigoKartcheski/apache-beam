import ast
import pandas as pd
from google.cloud import bigquery

class Tools:

    def __init__(self, tabela: str, path_to_raw_controller: str):

        self.tabela = tabela

        df = pd.read_csv(path_to_raw_controller)

        df_json = df.to_dict('records')

        self.table_line_filtred = list(filter(lambda doc: (doc['target_table_name'] == self.tabela and doc['load_controller'] == 1), df_json))[0]

    def get_schema_bq(self, simple_schema_str=False):
        fields_mapping = self.table_line_filtred.get('field_mapping')
        fields_mapping_list = ast.literal_eval(fields_mapping)
        field_mapping_formated = [(t[0], 'integer') if t[1] == 'int' or t[1] == 'bigint' else t for t in fields_mapping_list]

        if simple_schema_str:
            return ', '.join([f"{t[0]}:{t[1].upper()}" for t in field_mapping_formated])
        
        return [bigquery.SchemaField(t[0], t[1].upper()) for t in field_mapping_formated]

    def get_patition_field(self):
        return self.table_line_filtred.get('partition_field')
    
    def get_table_target(self):
        return self.table_line_filtred.get('target_table_name')
    
    def create_table_bq(self, client, table_id):
        table = bigquery.Table(table_id, schema=self.get_schema_bq(simple_schema_str=False))
        
        table = client.create_table(table)
