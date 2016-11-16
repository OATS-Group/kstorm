import logging
import time
import io

import simplejson as json

# zookeeper
from kazoo.client import KazooClient

# kafka
from pykafka import KafkaClient
from pykafka.common import OffsetType

# storm
from streamparse.spout import Spout

# avro
import avro.schema
import avro.io

# avro to json
from avro_json_serializer import AvroJsonSerializer

log = logging.getLogger(__name__)
schema_path = '/home/yang/source/kstorm/raw_can.avsc'

class KafkaReaderSpout(Spout):
    """Generic reader spout for Kafka queues.

    The base form of this spout will blindly read from a queue,
    unpack the data (assuming json) and emit that as a tuple
    with the field name ``data``.

    ``is_ok`` can be overridden to provide a quality check before any
    tuples are emitted.

    :param topic_name: Topic to read from
    :param consumer_group: Consumer group to register with in zookeeper
    :param commit_interval: Time in seconds between committing message offsets
    :param max_retries: Max retries for a tuple before declaring it a failure

    :output: ['data']
    """

    outputs = ['data']

    def initialize(self, stormconf, context):
        self._counter = 0
        self._statsd = None

        # kafka stuff
        self.topic_name = 'ibeng'
        self.consumer_group = 'stormtest'
        self.kafka_hosts = 'localhost:9092'
        self.zookeeper_hosts = 'localhost:2181'
        self.offset_reset = 'earliest'

        # initialize the consumer
        self.consumer = self.initializeKafka()

        # load avro schema
        self.schema = avro.schema.parse(open(schema_path).read())

    def initializeKafka(self):
        # connect to zookeeper
        zk = KazooClient(hosts=self.zookeeper_hosts)
        zk.start()

        # connect to broker and set up the consumer
        client = KafkaClient(hosts=self.kafka_hosts)
        topic = client.topics[self.topic_name]
        consumer = topic.get_balanced_consumer(consumer_group=self.consumer_group,
                                                consumer_timeout_ms=100,
                                                rebalance_max_retries=30,
                                                auto_offset_reset=OffsetType.EARLIEST,
                                                auto_commit_enable=False,
                                                zookeeper=zk)

        return consumer

    def get_data(self, unpacked):
        """Get list of data to emit. Override to change what's sent out"""
        return [unpacked]

    def get_stream(self, data):
        """Get the output stream for the tuple, based on the unpacked data."""
        # default stream
        return None

    def is_ok(self, unpacked):
        """Evaluate if ``unpacked`` is okay to pass on. Override to enable."""
        return True

    def emit_next(self):
        """Called to emit the next message from the Kafka topic.

        This can be overriden if specific behavior is needed from Kafka.
        For example, this could be overriden to group messages for a
        few seconds before emitting multiple messages to Storm. In theory,
        this could provide performance benefits.
        """
        if self._statsd:
            self._statsd.incr('calls.emit_next', sample_rate=0.01)
        msg = self.consumer.consume()
        if not msg:
            # nothing here
            return

        try:
            bytes_reader = io.BytesIO(msg.value)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(self.schema)
            decoded_data = reader.read(decoder)
            serializer = AvroJsonSerializer(self.schema)
            unpacked = serializer.to_json(decoded_data)
        except:
            log.error('Error in unpacking message: %s', msg)
            # drop the tuple
            return
        if self.is_ok(unpacked):
            data = self.get_data(unpacked)
            stream = self.get_stream(data)
            self.emit_tuple(data, stream=stream)
        else:
            log.debug('Unpacked message not okay: %s', msg)

    def emit_tuple(self, values, stream=None):
        """Attach and id and emit a list/tuple to Storm"""
        if self._statsd:
            self._statsd.incr('calls.emit_tuple', sample_rate=0.01)
        if self._counter == 0:
            # this is the first tuple, we like to sleep for
            # a few seconds to ensure that async connections
            # are all wired up in bolts before emitting
            time.sleep(3)
        self._counter += 1
        self.emit(values, tup_id=self._counter, stream=stream)

    def next_tuple(self):
        """Called by Storm to get the next tuple"""
        if self._statsd:
            self._statsd.incr('calls.next_tuple', sample_rate=0.01)
        self.emit_next()
