import apache_beam as beam

def explode_values(element):
    name, data = element

    icons = data['icons']
    durations = data['durations']

    if not icons:  # Verificar se a lista de ícones está vazia
        icons = [None]  # Substituir por uma lista com um valor nulo

    if not durations:  # Verificar se a lista de durações está vazia
        durations = [None]  # Substituir por uma lista com um valor nulo

    for icon in icons:
        for duration in durations:
            yield (name, {'icon': icon, 'duration': duration})



with beam.Pipeline() as pipeline:
    icon_pairs = pipeline | 'Create icons' >> beam.Create([
        ('Apple', {'1', 'a'}),
        ('Apple', {'2', 'b'}),
        ('Eggplant', {'🍆', 'c'}),
        ('Tomato', {'🍅', 'd'}),
    ])

    duration_pairs = pipeline | 'Create durations' >> beam.Create([
        ('Apple', {'perennial', 'a'}),
        ('Carrot', {'biennial', 'a'}),
        ('Tomato', {'perennial', 'a'}),
        ('Tomato', {'annual', 'a'}),
    ])

    plants = (
        ({
            'icons': icon_pairs,
            'durations': duration_pairs
        })
        | 'Merge' >> beam.CoGroupByKey()
        | 'ExplodeValues' >> beam.ParDo(explode_values)
        | beam.Map(print)
    )
