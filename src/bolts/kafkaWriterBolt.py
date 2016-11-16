import logging
import simplejson as json

from streamparse.bolt import Bolt

log = logging.getLogger(__name__)

class KafkaWriterBolt(Bolt):

    def initialize(self, conf, ctx):
        self.counter = 0

    def process(self, tup):
        sentence = json.loads(tup.values[0])
        self.counter += 1
	timestamp = sentence.get('timestamp')
	print timestamp, counter
	self.emit([timestamp, counter])
