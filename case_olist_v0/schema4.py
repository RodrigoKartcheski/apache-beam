import apache_beam as beam
import pyarrow


    

dados1 = [{'name': 'foo', 'age': 10}, 
          {'name': 'bar', 'age': 20}]

dados2 = [
    {'custumers_id': '821a7275a08f32975caceff2e08ea262'}, {'customers_unique_id': '046470763123d3d6364f89095b4e47ab'}, {'zip_code': '05734'},
    {'custumers_id': 'c6ece8a5137f3c9c3a3a12302a19a2ac'}, {'customers_unique_id': 'aaf22868003377e859049dcf5f0b3fdf'}, {'zip_code': '01323'},
    {'custumers_id': 'e5ed7280cd1a3ac2ba29fd6650d8867c'}, {'customers_unique_id': '206e64e8af2633a2ebe158a7fcb860db'}, {'zip_code': '08560'},
    {'custumers_id': '0a7db3996b88954c7aa763b5dd621d5b'}, {'customers_unique_id': '15637b62dfa4c5a9df846b22beef0994'}, {'zip_code': '52090'},
    {'custumers_id': '935993f47af1ed7d0715c26b686341c5'}, {'customers_unique_id': '4452b8ef472646c4cc042cb31a291f3b'}, {'zip_code': '12236'},
    {'custumers_id': '592b8900e0e8325027d885e6d30d0283'}, {'customers_unique_id': '57c2cfb4a80b13ed19b5fb258d29c19d'}, {'zip_code': '15720'},
]

dados3 = [
    {'custumers_id': '821a7275a08f32975caceff2e08ea262', 'customers_unique_id': '046470763123d3d6364f89095b4e47ab', 'zip_code': '05734','price':'122.09'},
    {'custumers_id': 'c6ece8a5137f3c9c3a3a12302a19a2ac', 'customers_unique_id': 'aaf22868003377e859049dcf5f0b3fdf', 'zip_code': '01323','price':'150.09'},
    {'custumers_id': 'e5ed7280cd1a3ac2ba29fd6650d8867c', 'customers_unique_id': '206e64e8af2633a2ebe158a7fcb860db', 'zip_code': '08560','price':'200.10'},
    {'custumers_id': '0a7db3996b88954c7aa763b5dd621d5b', 'customers_unique_id': '15637b62dfa4c5a9df846b22beef0994', 'zip_code': '52090','price':'210.11'},
    {'custumers_id': '935993f47af1ed7d0715c26b686341c5', 'customers_unique_id': '4452b8ef472646c4cc042cb31a291f3b', 'zip_code': '12236','price':'211.00'},
    {'custumers_id': '592b8900e0e8325027d885e6d30d0283', 'customers_unique_id': '57c2cfb4a80b13ed19b5fb258d29c19d', 'zip_code': '15720','price':'200.00'}
]

#mapping = [('name', pyarrow.binary()), ('age', pyarrow.int64())]
mapping1 = [('name', 'STRING'), ('age', 'int64')]
mapping2 = [('custumers_id', 'STRING'), ('customers_unique_id', 'STRING'), ('zip_code', 'STRING')]
mapping3 = [('custumers_id', 'STRING'), ('customers_unique_id', 'STRING'), ('zip_code', 'STRING'), ('price', 'double')]

# Função para converter string para float
def string_to_float(element):
    element['price'] = float(element['price'])
    return element


# Define uma pipeline do Beam
with beam.Pipeline() as p1:
  records = (p1 | 'Read' >> beam.Create(dados3)
               #| beam.Map(string_to_float)
               | 'Convert to float' >> beam.Map(string_to_float)
               )


  _ = records | 'Write' >> beam.io.WriteToParquet('/home/rodrigo/opt/apache_beam/case_olist/parquet/arquivo.parquet',
      pyarrow.schema(
          mapping3
      )
  )
