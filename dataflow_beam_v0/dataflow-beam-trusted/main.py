import logging
import apache_beam as beam
from beam import Tools, ReadBQ, MergeBQ, TrustedOptions
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

def main():
    pipeline_options = PipelineOptions()
    pipe_options = pipeline_options.view_as(TrustedOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    project = pipeline_options.display_data().get('project')
    path_controller = pipe_options.path_controller.get()

    tools = Tools(project, path_controller)

    tables_all = tools.get_tables()
    num_tables_per_list = 4

    with beam.Pipeline(options=pipeline_options) as pipe:
        for tables in range(0, len(tables_all), num_tables_per_list):
            dados = (
                pipe
                    | f'[TASK_ID-{tables}] Get name table' >> beam.Create(tables_all[tables:tables+num_tables_per_list]) 
                    | f'[TASK_ID-{tables}] Select table BQ' >> beam.ParDo(ReadBQ(project, path_controller)) 
                    | f'[TASK_ID-{tables}] Merge BQ' >> beam.ParDo(MergeBQ(project, path_controller))
            )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()