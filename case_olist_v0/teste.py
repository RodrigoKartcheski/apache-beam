import apache_beam as beam

dados = [
    {'custumers_id': '821a7275a08f32975caceff2e08ea262', 'customers_unique_id': '046470763123d3d6364f89095b4e47ab', 'zip_code': '05734'},
    {'custumers_id': 'c6ece8a5137f3c9c3a3a12302a19a2ac', 'customers_unique_id': 'aaf22868003377e859049dcf5f0b3fdf', 'zip_code': '01323'},
    {'custumers_id': 'e5ed7280cd1a3ac2ba29fd6650d8867c', 'customers_unique_id': '206e64e8af2633a2ebe158a7fcb860db', 'zip_code': '08560'},
    {'custumers_id': '0a7db3996b88954c7aa763b5dd621d5b', 'customers_unique_id': '15637b62dfa4c5a9df846b22beef0994', 'zip_code': '52090'},
    {'custumers_id': '935993f47af1ed7d0715c26b686341c5', 'customers_unique_id': '4452b8ef472646c4cc042cb31a291f3b', 'zip_code': '12236'},
    {'custumers_id': '592b8900e0e8325027d885e6d30d0283', 'customers_unique_id': '57c2cfb4a80b13ed19b5fb258d29c19d', 'zip_code': '15720'}
]

# Definir a função de transformação
def transform_data(element):
    # Aplicar transformações nos dados
    # Converter o valor do campo 'zip_code' em inteiro
    element['zip_code'] = str(int(element['zip_code']))
    return element

# Definir o pipeline
with beam.Pipeline() as pipeline:
    # Ler os dados de entrada
    data = pipeline | beam.Create(dados)
    
    # Realizar transformações nos dados
    transformed_data = data | beam.Map(transform_data)
    
    # Escrever os resultados
    transformed_data | beam.Map(print)
