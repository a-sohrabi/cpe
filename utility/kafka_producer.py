from confluent_kafka import Producer

from .config import settings
from .logger import LogManager

logger = LogManager('kafka_producer.py')


class KafkaProducer:
    def __init__(self, config):
        self.producer = Producer({
            'bootstrap.servers': config,
            'queue.buffering.max.messages': 100_000,  # Increase buffer size for batching
            'queue.buffering.max.ms': 100,  # Reduce the maximum time to buffer messages (default is 1000ms)
            'batch.num.messages': 10_000,  # Increase the batch size to reduce network overhead
            'compression.codec': 'gzip',  # Use gzip for compression to reduce payload size
            'linger.ms': 50,  # Reduce latency by waiting for a few milliseconds before sending a batch
            'message.max.bytes': 10485760,  # Max message size (10MB)
            'message.timeout.ms': 600000,  # Timeout for message delivery
            'acks': 'all',  # Wait for leader acknowledgement (use 'all' for stronger consistency)
            'enable.idempotence': True  # Ensure idempotence for safer retries and exactly-once semantics
        })
        self.message_queue = []

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(err)

    def add_message(self, topic, key, value):
        self.message_queue.append((topic, key, value))
        if len(self.message_queue) >= 1_000:
            self.flush()

    def flush(self):
        for topic, key, value in self.message_queue:
            self.producer.produce(topic, key=key, value=value, callback=self.delivery_report)
        self.producer.flush()
        self.message_queue = []


producer = KafkaProducer(settings.KAFKA_BOOTSTRAP_SERVERS)
