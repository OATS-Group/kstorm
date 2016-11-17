# zookeeper
from kazoo.client import KazooClient

# kafka
from pykafka import KafkaClient
from pykafka.common import OffsetType

from streamparse.bolt import Bolt

class KafkaWriterBolt(Bolt):

    def initialize(self, stormconf, context):

        # kafka stuff
        self.kafka_hosts = 'localhost:9092'
        self.zookeeper_hosts = 'localhost:2181'

        # initialize the producer 
        self.producer = self.initializeKafka()

    def initializeKafka(self):

        # connect to zookeeper
        zk = KazooClient(hosts=self.zookeeper_hosts)
        zk.start()

        # connect to broker and set up the producer
        client = KafkaClient(hosts=self.kafka_hosts)
        topic = client.topics[self.topic_name]

        producer = topic.get_producer()

        return producer 
