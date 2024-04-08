import apache_beam as beam

# Definir os dados
data = [
    {'No': 'No', 'Name': 'Name', 'Sal': 'Sal', 'Address': 'Address', 'Dept': 'Dept', 'Join_Date': 'Join_Date'},
    {'No': '11', 'Name': 'Sam', 'Sal': '1000', 'Address': 'ind', 'Dept': 'IT', 'Join_Date': '02/11/19'},
    {'No': '22', 'Name': 'Tom', 'Sal': '2000', 'Address': 'usa', 'Dept': 'HR', 'Join_Date': '02/11/19'},
    {'No': '33', 'Name': 'Can', 'Sal': '3500', 'Address': 'uk', 'Dept': 'IT', 'Join_Date': '02/11/19'},
    {'No': '50', 'Name': 'Sam', 'Sal': '1000', 'Address': 'ind', 'Dept': 'PM', 'Join_Date': '02/11/19'}
]

# Função para converter um dicionário em uma tupla com a chave "Name"
'''
def dict_to_tuple(dictionary):
    name = dictionary['Name']
    return (name, dictionary)
'''

def dict_to_tuple(dictionary):
    name = (dictionary['Name'], dictionary['No'])
    return (name, dictionary)
    
# Configurar o pipeline
with beam.Pipeline() as pipeline:
    # Criar um PCollection a partir dos dados
    data_collection = pipeline | 'CreateData' >> beam.Create(data)

    # Converter cada dicionário em uma tupla com a chave "Name"
    formatted_data = data_collection | 'FormatData' >> beam.Map(dict_to_tuple)

    # Aplicar a operação cogroupbykey
    grouped_data = formatted_data | 'GroupData' >> beam.GroupByKey()

    # Imprimir o resultado
    grouped_data | 'PrintResult' >> beam.Map(print)
