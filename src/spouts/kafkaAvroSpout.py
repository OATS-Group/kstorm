import io

import avro.schema
import avro.io

from spouts.kafkaReaderSpout import KafkaReaderSpout

# avro schema path
schema_path = '/home/yang/source/kstorm/raw_can.avsc'
schema = avro.schema.parse(open(schema_path).read())

class KafkaAvroSpout(KafkaReaderSpout):

    def get_data(self, msg):

        bytes_reader = io.BytesIO(msg)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        decoded = reader.read(decoder)
        decoded['data'] = decoded['data'].encode('hex')

        self.logger.debug([decoded])

        return [decoded]
