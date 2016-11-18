import os

from streamparse.bolt import Bolt

class PtoRpmBolt(Bolt):
    
    def initialize(self, conf, ctx):
        self.pid = os.getpid()

    def process(self, tup):
        self.logger.debug(tup.values.data)
