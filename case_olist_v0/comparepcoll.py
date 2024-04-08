import apache_beam as beam

#################### resultado e aprovado
class toolsMerge:        
    @staticmethod
    def compare_data(element):
        key, values = element

        data1 = values['data1']
        data2 = values['data2']

        # Comparar data1 com data2
        if data1 == data2:
            comparison_result = "suprime"
        elif not data1:
            comparison_result = "delete"
        elif not data2:
            comparison_result = "insert"
        else:
            comparison_result = "update"

        updated_values = dict(values)
        updated_values[comparison_result] = True

        return (key, updated_values)


    @staticmethod
    def inserir_chave(elemento): #insere a chave na tupla
        chave, dicionario = elemento
        chave3 = list(dicionario.keys())[2] if len(dicionario) > 2 else None
        if chave3:
            lista_tuplas1 = dicionario['data1']
            for i, tupla in enumerate(lista_tuplas1):
                lista_tuplas1[i] = tupla + (chave3,)  # Inserir a terceira chave na tupla da chave 'data1'
            
            lista_tuplas2 = dicionario['data2']
            for i, tupla in enumerate(lista_tuplas2):
                lista_tuplas2[i] = tupla + (chave3,)  # Inserir a terceira chave na tupla da chave 'data2'
        
        return chave, dicionario
    '''
    @staticmethod
    def inserir_chave_list(elemento): #insere a chave na lista
        chave, dicionario = elemento
        chave3 = list(dicionario.keys())[2] if len(dicionario) > 2 else None
        if chave3:
            dicionario['data1'].append(chave3)  # Inserir chave3 na lista de "data1"
            dicionario['data2'].append(chave3)  # Inserir chave3 na lista de "data2"
        return chave, dicionario'''
    '''
    @staticmethod
    def concatenate_columns(row):
        col1 = row['column1']
        col2 = row['column2']
        concatenated = col1 + col2
        row['concatenated'] = concatenated
        del row['column1']
        del row['column2']
        return row'''
        
    '''
    def explode_tuples(element):
        key, values = element
        data1_list = values.get('data1', [])
        data2_list = values.get('data2', [])

        for data1 in data1_list:
            updated_values = {'data1': [data1]}
            if 'suprime' in values:
                updated_values['suprime'] = values['suprime']
            yield (key, updated_values)

        for data2 in data2_list:
            updated_values = {'data2': [data2]}
            if 'delete' in values:
                updated_values['delete'] = values['delete']
            if 'insert' in values:
                updated_values['insert'] = values['insert']
            if 'update' in values:
                updated_values['update'] = values['update']
            yield (key, updated_values)'''
            
            
            
'''
class ExplodeDataFn(beam.DoFn):
    def process(self, element):
        key, values = element
        data1_list = values.get('data1', [])
        data2_list = values.get('data2', [])

        if 'suprime' in values:
            del values['suprime']  # Remove a terceira chave 'suprime'

        for data1 in data1_list:
            updated_values = {'data1': [data1]}
            yield (key, updated_values)

        for data2 in data2_list:
            updated_values = {'data2': [data2]}
            yield (key, updated_values)

class FilterData1Insert(beam.DoFn):
    def process(self, element):
        key, value = element
        if 'data1' in value and any(item[5] == 'update' for item in value['data1']):
            yield element
'''
            
class ToolsMergeFn(beam.DoFn):
    def explode_data(self, element):
        key, values = element
        data1_list = values.get('data1', [])
        data2_list = values.get('data2', [])

        if 'suprime' in values:
            del values['suprime']  # Remove a terceira chave 'suprime'

        for data1 in data1_list:
            updated_values = {'data1': [data1]}
            yield (key, updated_values)

        for data2 in data2_list:
            updated_values = {'data2': [data2]}
            yield (key, updated_values)
            
    def filter_type(self, element):
        key, value = element
        if 'data1' in value and any(item[5] == 'update' for item in value['data1']):
            yield element

# Dados de exemplo
data = [
    (11, {'data1': [('Sam', 1000, 'ind', 'IT', '02/11/19')], 'data2': [('Sam', 1000, 'ind', 'IT', '02/11/19')]}),
    (22, {'data1': [('Tom', 2000, 'usa', 'HR', '02/11/19')], 'data2': [('Tom', 2000, 'usa', 'HR', '12/11/19')]}),
    (33, {'data1': [('Can', 3500, 'uk', 'IT', '02/11/19')], 'data2': [('Can', 3500, 'uk', 'IT', '02/11/19')]}),
    (77, {'data1': [], 'data2': [('samy', 1000, 'ind', 'PM', '02/11/19')]}),
    (66, {'data1': [('samy', 1000, 'ind', 'PM', '02/11/19')], 'data2': []}),
]

with beam.Pipeline() as pipeline:
    results = (
            pipeline
            | beam.Create(data)
            | beam.Map(toolsMerge.compare_data)
            | beam.Map(toolsMerge.inserir_chave)
            | beam.ParDo(ToolsMergeFn().explode_data)
            | 'filter' >> beam.ParDo(ToolsMergeFn().filter_type)
            #| beam.ParDo(ExplodeDataFn())
            #| beam.ParDo(FilterData1Insert())
            ###| 'Concatenate columns' >> beam.Map(toolsMerge.concatenate_columns)
            #| beam.Filter(lambda element: 'update' in element[1])
            #| beam.Map(lambda element: print(element))
            | beam.Map(print)
        )


'''
import apache_beam as beam

data = [
    (11, {'data1': [('Sam', 1000, 'ind', 'IT', '02/11/19')], 'data2': [('Sam', 1000, 'ind', 'IT', '02/11/19')], 'suprime': True}),
    (22, {'data1': [('Tom', 2000, 'usa', 'HR', '02/11/19')], 'data2': [('Tom', 2000, 'usa', 'HR', '12/11/19')], 'update': True}),
    (33, {'data1': [('Can', 3500, 'uk', 'IT', '02/11/19')], 'data2': [('Can', 3500, 'uk', 'IT', '02/11/19')], 'suprime': True}),
    (77, {'data1': [], 'data2': [('samy', 1000, 'ind', 'PM', '02/11/19')], 'delete': True}),
    (66, {'data1': [('samy', 1000, 'ind', 'PM', '02/11/19')], 'data2': [], 'insert': True})
]

class ExplodeDataFn(beam.DoFn):
    def process(self, element):
        key, values = element
        data1_list = values.get('data1', [])
        data2_list = values.get('data2', [])

        for data1 in data1_list:
            updated_values = {'data1': [data1]}
            if 'suprime' in values:
                updated_values['suprime'] = values['suprime']
            yield (key, updated_values)

        for data2 in data2_list:
            updated_values = {'data2': [data2]}
            if 'delete' in values:
                updated_values['delete'] = values['delete']
            if 'insert' in values:
                updated_values['insert'] = values['insert']
            if 'update' in values:
                updated_values['update'] = values['update']
            yield (key, updated_values)



with beam.Pipeline() as pipeline:
    results = (
        pipeline
        | beam.Create(data)
        | beam.ParDo(ExplodeDataFn())
        | beam.Map(print)
    )
'''

'''
import apache_beam as beam

def extrair_chaves(elemento):
    _, dicionario = elemento  # Desempacota o elemento em (_, dicionario)
    chaves = list(dicionario.keys())  # Obtém as chaves do dicionário
    terceira_chave = chaves[2] if len(chaves) > 2 else None  # Obtém a terceira chave (ou None se não existir)
    return terceira_chave

# Dados de exemplo
dados = [
    (11, {'data1': [('Sam', 1000, 'ind', 'IT', '02/11/19')], 'data2': [('Sam', 1000, 'ind', 'IT', '02/11/19')], 'suprime': True}),
    (22, {'data1': [('Tom', 2000, 'usa', 'HR', '02/11/19')], 'data2': [('Tom', 2000, 'usa', 'HR', '12/11/19')], 'update': True}),
    (33, {'data1': [('Can', 3500, 'uk', 'IT', '02/11/19')], 'data2': [('Can', 3500, 'uk', 'IT', '02/11/19')], 'suprime': True}),
    (77, {'data1': [], 'data2': [('samy', 1000, 'ind', 'PM', '02/11/19')], 'delete': True}),
    (66, {'data1': [('samy', 1000, 'ind', 'PM', '02/11/19')], 'data2': [], 'insert': True})
]

# Pipeline do Apache Beam
pipeline = beam.Pipeline()

# Aplica a transformação Map para extrair as chaves da terceira chave
chaves_terceira = (
    pipeline
    | "Crie o PCollection" >> beam.Create(dados)
    | "Extrair chaves da terceira chave" >> beam.Map(extrair_chaves)
    | beam.Map(print)
)

# Executa o pipeline e obtém os resultados
resultados = pipeline.run()

'''

