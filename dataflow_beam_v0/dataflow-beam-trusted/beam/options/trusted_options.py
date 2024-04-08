from apache_beam.options.pipeline_options import PipelineOptions

class TrustedOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_value_provider_argument(
                                    '--path_controller',
                                    required=True,
                                    type=str
        )