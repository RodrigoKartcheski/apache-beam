from apache_beam import PTransform, ParDo, DoFn, Create
from kafka import KafkaConsumer
import apache_beam as beam

class KafkaC(PTransform):

    def __init__(self, bootstrap_servers: list, topic: str, offset = 'latest'):
        self._consumer_config = {
            'bootstrap_servers': bootstrap_servers,
            'topic': topic,
            'offset': offset
        }

    def expand(self, pcoll):
        consumer_config = (pcoll | Create([self._consumer_config]))

        consumer_1 = (consumer_config | 'Consumer 1' >> ParDo(_KafkaConsumerParDo()))
        # consumer_2 = (consumer_config | 'Consumer 2' >> ParDo(_KafkaConsumerParDo()))

        # merge_consumers = (consumer_1, consumer_2) | beam.Flatten()

        return consumer_1

class _KafkaConsumerParDo(DoFn):

    def process(self, args: dict):
        topic = args.get('topic')
        servers = args.get('bootstrap_servers')
        offset = args.get('offset')

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=servers,
            auto_offset_reset=offset
        )

        for msg in consumer:
            try:
                yield msg.key, bytes.decode(msg.value)
            except Exception as e:
                print(e)
                continue