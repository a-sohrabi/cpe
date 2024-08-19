from confluent_kafka import Producer

from .config import settings
from .logger import logger


class KafkaProducer:
    def __init__(self, config):
        self.producer = Producer({'bootstrap.servers': config})

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    def send(self, topic, key, value):
        self.producer.produce(topic, key=key, value=value, callback=self.delivery_report)
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()


producer = KafkaProducer(settings.KAFKA_BOOTSTRAP_SERVERS)