from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import ReadFromJdbc
import apache_beam as beam
import os
import pyarrow as pa
import numpy as np
import decimal
from apache_beam.io.parquetio import WriteToParquet
from google.cloud import storage
import pyarrow as pa

pipeline_options = {
    'project': 'data-analytics-405123',
    'runner': 'DataflowRunner',
    'region': 'us-central1',
    'staging_location': 'gs://dataflow-central1/staging',
    'temp_location': 'gs://dataflow-central1/temp',
    'template_location': 'gs://dataflow-central1/templates/jdbc-conection-oracle-v4',
    'subnetwork': 'https://www.googleapis.com/compute/v1/projects/vpc-host-prod-eh839-gx617/regions/us-central1/subnetworks/us-grp-jacto-data-analytics-subnet',  # Substitua com as informações da sua subrede
    'save_main_session': True 
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
#p1 = beam.Pipeline(options=pipeline_options)
p1 = beam.Pipeline()

# Caminho para o arquivo de credenciais do Google Cloud
serviceAccount = r'/home/michel/Documentos/DataSide/GRUPO_JACTO/dev/Oracle_Beam_JDBC/keys/data-analytics-405123-1568a08f6dd9.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

table_name = 'origem'


# Caminho do arquivo no Google Cloud Storage
bucket_name = "dataflow-central1"
file_path = f"/home/rodrigo/opt/apache_beam/case_olist/{table_name}.txt"
#gcs_file_path = f"gs://{bucket_name}/{file_path}"

'''
def read_txt_from_gcs(file_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    content = blob.download_as_text()
    return content

text_content = read_txt_from_gcs(file_path)
'''

# Now iterate over each line and print
# Function to generate Parquet schema from text content
def generate_parquet_schema(text_content):
    schema_fields = []
    for line in text_content.split('\n'):
        if line.strip():  # Skip empty lines
            field_name, field_type = line.strip('()').split(',')
            field_name = field_name.strip().strip("'")
            field_type = field_type.strip().strip("'").lower()
            if field_type in ['number', 'int', 'bigint', 'smallint', 'tinyint']:
                schema_fields.append((field_name, pa.int64()))
            elif field_type in ['varchar2', 'varchar', 'char', 'text']:
                schema_fields.append((field_name, pa.string()))
            elif field_type in ['float', 'double', 'decimal']:
                schema_fields.append((field_name, pa.float64()))
            elif field_type in ['date', 'datetime', 'timestamp']:
                schema_fields.append((field_name, pa.timestamp('ms')))
            # Add more data types as needed
            
    return pa.schema(schema_fields)


#parquet_schema = generate_parquet_schema(text_content)


#row = [line.split(',')[0].strip("('") for line in text_content.split('\n') if line.strip()]


def create_dict(row):
    return {
        row: f"row[{i}] if row[{i}] is not None else '0'" 
        for i, row in enumerate(row, start=1)
    }

def map_to_dict():
    d = create_dict(row)
    a = str(d).replace('"', '')
    return dict(a)

d = map_to_dict()
print(d)

"""
# Função para lidar com objetos decimal.Decimal
def handle_decimal(value):
    if isinstance(value, decimal.Decimal):
        return float(value)  # Ou qualquer outra conversão que você precise
    return value

# Mapear para um dicionário com tratamento de decimal.Decimal
def map_to_dict_with_decimal_handling(row):
    return {key: handle_decimal(value) for key, value in map_to_dict().items()}


(file_path_prefix, file_name_suffix) = (f'gs://dataflow-central1/JDBC/tables-data/{table_name}.parquet', '')
num_shards = 1  # Definir o número de shards como 1 para um único arquivo Parquet

get_data = (
    p1
    | f'Read from jdbc schema - {table_name}' >> ReadFromJdbc(
        fetch_size=None,
        table_name=table_name,
        driver_class_name='oracle.jdbc.driver.OracleDriver',
        jdbc_url='jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.4.15.249)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=prod)))',
        username='SV_GCP_DATA',
        password='4F$R1TX)}+UQMX^6xM=i',
        query=f"SELECT * FROM DWJACTO.{table_name} WHERE ROWNUM < 10"
    )
    | 'replace to zero' >> beam.Map(lambda x: x if x not in ['', ' ', 'None', None, 'nan', np.nan] else 'NULL')
    | 'Map to dictionary with decimal handling' >> beam.Map(map_to_dict_with_decimal_handling)
)

# Escrever no Parquet
get_data | 'Write to Parquet' >> WriteToParquet(
    file_path_prefix,
    file_name_suffix=file_name_suffix,
    num_shards=num_shards,
    schema=parquet_schema,
    shard_name_template=''
)

p1.run()
"""
