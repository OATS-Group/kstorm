from bolts.kafkaWriterBolt import KafkaWriterBolt

class EngineRpmBolt(KafkaWriterBolt):

    def __init__(self, *args, **kwargs):

        super(EngineRpmBolt, self).__init__(*args, **kwargs)
        self.topic_name = 'engrpm'

    def process(self, tup):

        data = tup.values.data['payload_bytes']
        data = [x.encode('utf-8') for x in data]
        self.logger.debug(data)
        engrpm_bytes = b''.join((data[4], data[3]))
        engrpm = int(engrpm_bytes, 16) * 0.125

        self.producer.produce(str(tup.values.data['timestamp']) + ',' + \
                              str(engrpm))
