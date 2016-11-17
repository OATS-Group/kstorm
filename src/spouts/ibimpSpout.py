from spouts.kafkaAvroSpout import KafkaAvroSpout

class IbimpSpout(KafkaAvroSpout):
    
    def __init__(self, *args, **kwargs):
        super(IbimpSpout, self).__init__(*args, **kwargs)
        self.topic_name = 'ibimp'
