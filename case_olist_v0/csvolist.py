'''import apache_beam as beam
import csv

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
    columns = ['review_id', 'order_id', 'review_score', 'review_comment_title', 'review_comment_message', 'review_creation_date', 'review_answer_timestamp']  # Colunas do CSV

    # Utilize a biblioteca csv para processar a linha
    reader = csv.reader([line])
    values = next(reader)

    return dict(zip(columns, values))



# Define o pipeline
with beam.Pipeline() as pipeline:
    # Read the CSV file
    csv_lines = (
        pipeline
        | 'Read CSV' >> beam.io.ReadFromText('/home/rodrigo/opt/kaggle/transient/olist_order_reviews_dataset2.csv')
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




import apache_beam as beam

def convert_to_dict(row):
    headers = row[0]
    data = row[1]
    return dict(zip(headers, data))

def read_csv(filename):
    with beam.Pipeline() as pipeline:
        rows = (
            pipeline
            | 'Read From File' >> beam.io.ReadFromText('/home/rodrigo/opt/kaggle/transient/olist_order_reviews_dataset.csv')
            | 'Split Rows' >> beam.Map(lambda row: row.split(','))
            | 'Convert to Dict' >> beam.Map(convert_to_dict)
        )
        
        rows | beam.Map(print)
