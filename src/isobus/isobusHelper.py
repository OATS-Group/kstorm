'''
ISOBUS helper function to parse out raw CAN frames 
'''

import struct
import re

# ISOBUS message masks
MASK_2_BIT = ((1 << 2) - 1)
MASK_3_BIT = ((1 << 3) - 1)
MASK_8_BIT = ((1 << 8) - 1)

class IsobusHelper:

    def __init__(self):
        pass

    def parse(self, hex_message, timestamp=0):

        header_hex = hex_message[:8]
        header = int(header_hex, 16)

        src = header & MASK_8_BIT
        header >>= 8
        pdu_ps = header & MASK_8_BIT
        header >>= 8
        pdu_pf = header & MASK_8_BIT
        header >>= 8
        res_dp = header & MASK_2_BIT
        header >>= 2
        priority = header & MASK_3_BIT

        pgn = res_dp
        pgn <<= 8
        pgn |= pdu_pf
        pgn <<= 8
        if pdu_pf >= 240:
            # pdu format 2 - broadcast message. PDU PS is an extension of
            # the identifier
            pgn |= pdu_ps

        payload_bytes = re.findall('[0-9a-fA-F]{2}', hex_message[8:])
        payload_int = int(''.join(reversed(payload_bytes)), 16)

        return {'pgn': pgn,
                'payload_bytes': payload_bytes,
                'timestamp': timestamp}
