import asyncio
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Optional, List

import aiofiles
import pytz
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

from .database import cpe_collection
from .kafka_producer import producer
from .logger import LogManager
from .schemas import CPEResponse, CPECreate

tehran_tz = pytz.timezone('Asia/Tehran')

logger = LogManager('crud.py')

stats = {
    "inserted": 0,
    "updated": 0,
    "errors": 0,
    "last_called": None,
    "durations": None
}


async def get_cpe(cpe_name: str) -> Optional[CPEResponse]:
    document = await cpe_collection.find_one({"cpe_name": cpe_name})
    if document:
        return CPEResponse(**document)


async def bulk_create_or_update_cpes(cpes: List[CPECreate], batch_size=1000):
    global stats
    operations = []
    created_cpes = []
    updated_cpes = []
    cpe_objects = []

    for cpe in cpes:
        operations.append(
            UpdateOne(
                {"cpe_name": cpe.cpe_name},
                {"$set": cpe.dict()},
                upsert=True
            )
        )
        cpe_objects.append(cpe)  # Track the CPE object for each operation

        if len(operations) >= batch_size:
            await execute_bulk_write(operations, created_cpes, updated_cpes, cpe_objects)
            operations = []
            cpe_objects = []

    if operations:
        await execute_bulk_write(operations, created_cpes, updated_cpes, cpe_objects)

    await send_kafka_messages_in_batches(created_cpes, updated_cpes)


async def execute_bulk_write(operations, created_cpes, updated_cpes, cpe_objects):
    try:
        result = await cpe_collection.bulk_write(operations)

        matched_count = result.matched_count
        upserted_count = len(result.upserted_ids)

        # Match the operations with the corresponding CPE objects
        for i, cpe in enumerate(cpe_objects):
            if i in result.upserted_ids:
                created_cpes.append(cpe)
            else:
                updated_cpes.append(cpe)

        stats['inserted'] += upserted_count
        stats['updated'] += matched_count
    except BulkWriteError as bwe:
        logger.error(f"Bulk write error: {bwe.details}")
        stats['error'] += 1
    except Exception as e:
        logger.error(f"General error during bulk write: {str(e)}")
        stats['error'] += 1


async def send_kafka_messages_in_batches(created_cpes, updated_cpes, batch_size=100):
    created_batches = [created_cpes[i:i + batch_size] for i in range(0, len(created_cpes), batch_size)]
    updated_batches = [updated_cpes[i:i + batch_size] for i in range(0, len(updated_cpes), batch_size)]

    for batch in created_batches:
        for cpe in batch:
            producer.add_message('cpe.extract.created', key=str(cpe.cpe_name), value=cpe.json())
        await asyncio.to_thread(producer.flush)

    for batch in updated_batches:
        for cpe in batch:
            producer.add_message('cpe.extract.updated', key=str(cpe.cpe_name), value=cpe.json())
        await asyncio.to_thread(producer.flush)


async def reset_stats():
    global stats
    stats = {
        "inserted": 0,
        "updated": 0,
        "errors": 0,
        "last_called": None,
        "durations": None
    }


async def get_stats():
    return stats


def record_stats():
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
            except Exception as e:
                logger.error(e)
                result = None
            end_time = time.time()
            duration = end_time - start_time

            # Determine appropriate time unit
            minutes, seconds = divmod(duration, 60)
            hours, minutes = divmod(minutes, 60)

            human_readable_duration = (
                f"{hours:.2f} hours" if hours >= 1 else
                f"{minutes:.2f} minutes" if minutes >= 1 else
                f"{seconds:.2f} seconds"
            )

            start_time_dt = datetime.fromtimestamp(start_time, tz=pytz.utc).astimezone(tehran_tz)

            stats["last_called"] = start_time_dt.strftime('%Y-%m-%d %H:%M:%S')
            stats["durations"] = human_readable_duration

            return result

        return wrapper

    return decorator


async def read_version_file(version_file_path: Path) -> str:
    async with aiofiles.open(version_file_path, 'r') as file:
        version = await file.read()
    return version.strip()


async def read_markdown_file(markdown_file_path: Path) -> str:
    async with aiofiles.open(markdown_file_path, 'r') as file:
        content = await file.read()
    return content
