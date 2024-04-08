import apache_beam as beam
import datetime

def partition_by_year_month(element):
    date_str = element['date']
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    year_month = date.strftime('%Y%m')
    return (year_month, element)

def create_sample_data():
    # Dados fictícios
    data = [
        {'date': '2022-01-01', 'value': 10},
        {'date': '2022-01-02', 'value': 15},
        {'date': '2022-02-01', 'value': 20},
        {'date': '2022-02-02', 'value': 25},
        {'date': '2022-03-01', 'value': 30},
        {'date': '2022-03-02', 'value': 35}
    ]
    return data

pipeline = beam.Pipeline()

sample_data = (
    pipeline
    | "Criação dos dados fictícios" >> beam.Create(create_sample_data())
    | "Particionamento por anomes" >> beam.Map(partition_by_year_month)
    | "Escrita dos dados" >> beam.io.WriteToText(r"/home/rodrigo/Documentos/teste/a", file_name_suffix='.txt')
)

pipeline.run()