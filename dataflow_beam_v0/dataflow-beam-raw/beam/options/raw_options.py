from apache_beam.options.pipeline_options import PipelineOptions

class RawOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_value_provider_argument(
                                    '--dataset',
                                    required=True,
                                    type=str
        )

        parser.add_value_provider_argument(
                                    '--path_to_raw_controller',
                                    required=True,
                                    type=str
        )