import asyncio

from confluent_kafka import Producer

from .config import settings
from .logger import logger


class KafkaProducer:
    def __init__(self, config):
        self.producer = Producer({
            'bootstrap.servers': config,
            'linger.ms': 50,  # Wait up to 50ms to accumulate messages
            'batch.size': 65536,  # Batch size in bytes
            'queue.buffering.max.messages': 1_500_000,  # Max messages in memory before sending
            'queue.buffering.max.kbytes': 1048576,  # Max memory in KB before sending
        })

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")

    def send(self, topic, key, value):
        self.producer.produce(topic, key=key, value=value, callback=self.delivery_report)

    def flush(self):
        self.producer.flush()

    async def flush_periodically(self, interval=1):
        while True:
            await asyncio.sleep(interval)
            self.producer.poll(0)  # Trigger delivery callbacks
            self.producer.flush(0)  # Non-blocking flush


producer = KafkaProducer(settings.KAFKA_BOOTSTRAP_SERVERS)

asyncio.create_task(producer.flush_periodically())
