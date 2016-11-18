import os

from streamparse import Stream
from streamparse.bolt import Bolt

from isobus.isobusHelper import IsobusHelper

class IsobusBolt(Bolt):

    outputs = [Stream(['data'], 'engrpm'),
               Stream(['data'], 'ptorpm'),
              ]

    def initialize(self, conf, ctx):

        self.pid = os.getpid()
        self.pgns = {61444: 'engrpm',
                     65091: 'ptorpm'
                    }
        self.helper = IsobusHelper()

    def process(self, tup):

        arbitration_id = (hex(tup.values.data["arbitration_id"])[2:]).rjust(8, "0")
        raw_can_frame = arbitration_id + str(tup.values.data['data'])
        parsed_message = self.helper.parse(raw_can_frame, \
                                           tup.values.data['timestamp'])
        # get the pgn for the current tuple
        tup_pgn = parsed_message['pgn']

        self.logger.debug('Tuple PGN: %d', tup_pgn)

        if tup_pgn in self.pgns:
            self.logger.debug('PGN matches: %d, will go to stream: %s', \
                             tup_pgn, self.pgns[tup_pgn]) 
            self.emit([parsed_message], stream=self.pgns[tup_pgn])
