import apache_beam as beam

# Define a função para mapear cada linha do CSV para uma tupla (chave, valor)
def extract_key_value(line):
    values = line.split(',')
    return (values[0], values[1:])
    
def expand_lists(element):
    key, value = element

    if isinstance(value['joins'], list):
        for sublist in value['joins']:
            yield (key, {'joins': sublist})
            

def convert_to_csv(element):
    key, value = element

    joined_values = [key] + value['joins']
    csv_row = ','.join(joined_values)

    return csv_row

def parse_csv(line):
    columns = ['No', 'Name', 'Sal', 'Address', 'Dept', 'Join_Date']  # Colunas do CSV
    values = line.split(',')  # Separar os campos por vírgula
    return dict(zip(columns, values))


# Define o pipeline
with beam.Pipeline() as pipeline:
    # Read the CSV file
    csv_lines = (
        pipeline
        | 'Read CSV' >> beam.io.ReadFromText('/home/rodrigo/opt/apache_beam/case_olist/join_your.csv')
    )
    
    # Process the CSV data (you can modify this step according to your needs)
    processed_data = (
        csv_lines
        #| 'Process CSV' >> beam.Map(lambda line: line.split(','))
        | 'Parse CSV' >> beam.Map(parse_csv)
        | beam.Map(print)
    )
    
    # Duplicate the csv_lines PCollection
    duplicated_lines = (
        csv_lines
        | 'Duplicate CSV Lines' >> beam.Map(lambda x: x)
    )
    
    # Mapeia cada linha para uma tupla (chave, valor)
    key_value_pairs = (
        duplicated_lines
        | 'Extract Key-Value' >> beam.Map(extract_key_value)
    )
    '''
    grouped_data = (
        ({
            'joins': key_value_pairs
        })
        | 'Merge' >> beam.CoGroupByKey()
        | beam.Map(print)
    )'''
    '''
     # Aplica a transformação FlatMap para expandir as listas de listas
    expanded_data = (
        grouped_data 
        | beam.FlatMap(expand_lists)
    )
    
      # Converte para formato CSV
    csv_data = (
        expanded_data 
        | beam.Map(convert_to_csv)
        | beam.io.WriteToText('/home/rodrigo/opt/apache_beam/case_olist/output.csv', num_shards=1)
    )'''