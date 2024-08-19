import socket
import time

import aiohttp
from httpx import AsyncClient

from .config import settings
from .database import client
from .kafka_producer import producer
from .logger import logger, log_error


async def check_mongo():
    try:
        client.admin.command('ping')
        return True
    except ConnectionError as e:
        log_error(e, {'function': 'check_mongo', 'context': 'mongodb connection'})
        return False


async def check_kafka():
    try:
        test_topic = "test"
        test_key = "test_key"
        test_value = "test_value"

        producer.send(test_topic, test_key, test_value)
        producer.flush()

        return {"message": "Test message sent successfully!"}
    except Exception as e:
        log_error(e, {'function': 'check_kafka', 'context': 'connection to kafka'})
        return {"message": "Failed to send test message", "error": str(e)}


async def check_url(url):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    return True
                else:
                    logger.error(f"Failed to access {url}: {response.status}")
                    return False
        except Exception as e:
            log_error(e, {'function': 'check_url', 'context': 'checking cpe urls', 'input': url})
            return False


async def check_loki():
    try:
        test_log_message = "Loki health check"
        logger.info(test_log_message)
        async with AsyncClient() as client:
            response = await client.post(settings.LOKI_URL, json={
                "streams": [
                    {
                        "stream": {"application": "health_check"},
                        "values": [[str(int(time.time() * 1e9)), test_log_message]]
                    }
                ]
            })
            response.raise_for_status()
            return {"loki": "connected"}
    except Exception as e:
        log_error(e, {'function': 'check_loki', 'context': 'health check loki'})
        return {"loki": f"disconnected: {e}"}


async def check_internet_connection():
    try:
        socket.create_connection(("8.8.8.8", 53), timeout=2)
        return True
    except OSError as e:
        log_error(e,
                  {'function': 'check_internet_connection', 'context': 'check if the service is connected to internet'})
        return False
