import apache_beam as beam
import pyarrow as pa
from datetime import datetime


dados1 = [
    {'custumers_id': '821a7275a08f32975caceff2e08ea262', 'zip_code': '05734','price':'122.09','dta':'2011-01-01T00:02:00.000000'},
    {'custumers_id': 'c6ece8a5137f3c9c3a3a12302a19a2ac', 'zip_code': '01323','price':'150.09','dta':'2012-01-01T00:00:00.000000'},
    {'custumers_id': 'e5ed7280cd1a3ac2ba29fd6650d8867c', 'zip_code': '08560','price':'200.10','dta':'2012-01-01T00:00:00.000000'},
    {'custumers_id': '0a7db3996b88954c7aa763b5dd621d5b', 'zip_code': '52090','price':'210.11','dta':'2012-01-01T00:00:00.000000'},
    {'custumers_id': '935993f47af1ed7d0715c26b686341c5', 'zip_code': '12236','price':'211.00','dta':'2012-01-01T00:00:00.000000'},
    {'custumers_id': '592b8900e0e8325027d885e6d30d0283', 'zip_code': '15720','price':'200.00','dta':'2013-01-01T00:00:00.000000'}
]


dados2 = [
    {'custumers_id': '821a7275a08f32975caceff2e08ea262', 'zip_code': '05734','price':'122.09','dta':'2012-01-01'},
    {'custumers_id': 'c6ece8a5137f3c9c3a3a12302a19a2ac', 'zip_code': '01323','price':'150.09','dta':'2012-01-01'},
    {'custumers_id': 'e5ed7280cd1a3ac2ba29fd6650d8867c', 'zip_code': '08560','price':'200.10','dta':'2012-01-01'},
    {'custumers_id': '0a7db3996b88954c7aa763b5dd621d5b', 'zip_code': '52090','price':'210.11','dta':'2012-01-01'},
    {'custumers_id': '935993f47af1ed7d0715c26b686341c5', 'zip_code': '12236','price':'211.00','dta':'2012-01-01'},
    {'custumers_id': '592b8900e0e8325027d885e6d30d0283', 'zip_code': '15720','price':'200.00','dta':'2012-01-01'}
]

columns1 = [('custumers_id', 'STRING'), ('zip_code', 'STRING'), ('price', 'float'), ('dta', 'TIMESTAMP')]
columns2 = [('custumers_id', 'STRING'), ('zip_code', 'STRING'), ('price', 'float'), ('dta', 'DATE')]

# Mapeamento de tipos para pyarrow
pyarrow_types = {
    'STRING': pa.string(),
    'FLOAT': pa.float64(),
    'DATE': pa.date32(),
    'TIMESTAMP':pa.timestamp('ms')
}

columns = columns1

# Converter as colunas em um esquema pyarrow
fields = [pa.field(name, pyarrow_types[type_str.upper()]) for name, type_str in columns]
schema = pa.schema(fields)

# Função para converter string para float
def string_to_float(element):
    if isinstance(element['price'], str):  # Verifica se o valor é uma string
        element['price'] = element['price'].replace("'", "")  # Remove as aspas
        element['price'] = float(element['price'])  # Convertendo a string para float
    return element
    
# Função para converter string para data
def string_to_date1(element):
    element['dta'] = datetime.strptime(element['dta'], '%Y-%m-%d')
    return element
    
def string_to_date(element):
    # Converte a string para um objeto datetime
    element['dta'] = datetime.strptime(element['dta'], '%Y-%m-%dT%H:%M:%S.%f')
    return element




# Define uma pipeline do Beam
with beam.Pipeline() as p1:
  records = (p1 | 'Read' >> beam.Create(dados)
               #| beam.Map(string_to_float)
               | 'Convert to float' >> beam.Map(string_to_float)
               | 'Convert to date' >> beam.Map(string_to_date)
               #| 'print'  >> beam.Map(print)
               )
               
  _ = records | 'Write' >> beam.io.WriteToParquet('/home/rodrigo/opt/apache_beam/case_olist/parquet/arquivo.parquet',
        pa.schema(
          schema
      )
  )
