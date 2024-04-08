import apache_beam as beam
import pandas as pd

# Função para converter cada linha em um dicionário
def row_to_dict(row):
    return {
        'No': row[0],
        'Name': row[1],
        'Sal': row[2],
        'Address': row[3],
        'Dept': row[4],
        'Join_Date': row[5]
    }


# Função para mapear cada elemento para um par chave-valor
def add_key_value(element):
    return (element['key'], element)
    
# Create the data for df_source
data_source = [(11,'Samy',1000,'ind','IT','2/11/2019'),
               (22,'Tom',2000,'usa','HR','2/11/2019'),
               (33,'Kom',3000,'uk','IT','2/11/2019'),
               (44,'Nom',4000,'can','HR','2/11/2019'),
               (55,'Xom',5000,'mex','IT','2/11/2019'),
               (77,'XYZ',5000,'mex','IT','2/11/2019')]

# Create the data for df_target
data_target = [(11,'Sam',1000,'ind','IT','2/11/2019'),
               (22,'Tom',2000,'usa','HR','2/11/2019'),
               (33,'Kom',3500,'uk','IT','2/11/2019'),
               (44,'Nom',4000,'can','HR','2/11/2019'),
               (55,'Vom',5000,'mex','IT','2/11/2019'),
               (66,'XYZ',5000,'mex','IT','2/11/2019')]
columns_target = ['No','Name','Sal','Address','Dept','Join_Date']
df_target = pd.DataFrame(data_target, columns=columns_target)

columns_source = ['No','Name','Sal','Address','Dept','Join_Date']
df_source = pd.DataFrame(data_source, columns=columns_source)

# Crie as pcolls a partir dos DataFrames
with beam.Pipeline() as p:
    pcoll_source = (p
                    | 'Create PColl Source' >> beam.Create(df_source.values.tolist())
                    | 'Convert to Dict Source' >> beam.Map(row_to_dict)
)
    
    pcoll_target = (p
                    | 'Create PColl Target' >> beam.Create(df_target.values.tolist())
                    | 'Convert to Dict Target' >> beam.Map(row_to_dict)
)

   # Adicionar uma chave única para cada elemento
    pcoll_source_with_key = (pcoll_source
                             | 'Add Key Value Source' >> beam.Map(add_key_value, counter=beam.pvalue.AsSingleton(0)))

    pcoll_target_with_key = (pcoll_target
                             | 'Add Key Value Target' >> beam.Map(add_key_value, counter=beam.pvalue.AsSingleton(df_source.shape[0])))

    # Mesclar as duas PColl
    merged_pcoll = ((pcoll_source_with_key, pcoll_target_with_key)
                    | 'Merge PColls' >> beam.Flatten())

    # Aplicar o GroupByKey para realizar o groupby nas chaves
    grouped_pcoll = (merged_pcoll
                     | 'Group By Key' >> beam.GroupByKey())

    # Iterar sobre o resultado do groupby
    result_pcoll = (grouped_pcoll
                    | 'Iterate Grouped Data' >> beam.Map(print))

    p.run()
