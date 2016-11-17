from spouts.kafkaAvroSpout import KafkaAvroSpout

class IbengSpout(KafkaAvroSpout):
    
    def __init__(self, *args, **kwargs):
        super(IbengSpout, self).__init__(*args, **kwargs)
        self.topic_name = 'ibeng'
