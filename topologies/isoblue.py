"""
Word count topology
"""

from streamparse import Grouping, Topology

from bolts.kafkaWriterBolt import KafkaWriterBolt
from spouts.kafkaReaderSpout import KafkaReaderSpout 

class isoblue(Topology):

    kafka_spout = KafkaReaderSpout.spec(name='kafka_spout')
    kafka_bolt = KafkaWriterBolt.spec(name='kafka_bolt', \
                                      inputs={kafka_spout: Grouping.fields('data')})
