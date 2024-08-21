import asyncio
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Optional

import aiofiles
import pytz
from pydantic_core._pydantic_core import ValidationError
from pymongo.errors import BulkWriteError

from .config import settings
from .database import cpe_collection
from .kafka_producer import producer
from .logger import log_error
from .schemas import CPEResponse, CPECreate

tehran_tz = pytz.timezone('Asia/Tehran')

stats = {
    "inserted": 0,
    "updated": 0,
    "errors": 0,
    "last_called": None,
    "durations": None
}


async def get_cpe(cpe_id: str) -> Optional[CPEResponse]:
    document = await cpe_collection.find_one({"cpe_id": cpe_id})
    if document:
        return CPEResponse(**document)


semaphore = asyncio.Semaphore(10)


async def create_or_update_cpe(cpe: CPECreate):
    async with semaphore:
        global stats
        try:
            result = await cpe_collection.update_one(
                {"cpe_id": cpe.cpe_id},
                {"$set": cpe.dict()},
                upsert=True
            )
            if result.upserted_id:
                stats['inserted'] += 1
            else:
                stats['updated'] += 1

            try:
                producer.send(settings.KAFKA_TOPIC, key=str(cpe.cpe_id), value=cpe.json())
            except Exception as ke:
                log_error(ke, {'function': 'create_or_update_cpe', 'context': 'kafka producing', 'input': cpe.dict()})
                stats['errors'] += 1
        except ValidationError as e:
            log_error(e, {'function': 'create_or_update_cpe', 'context': 'pydantic validation', 'input': cpe.dict()})
            stats['errors'] += 1
        except BulkWriteError as bwe:
            log_error(bwe, {'function': 'create_or_update_cpe', 'context': 'bulk write error', 'input': cpe.dict()})
            stats['errors'] += 1
        except Exception as e:
            log_error(e, {'function': 'create_or_update_cpe', 'context': 'other exceptions', 'input': cpe.dict()})
            stats['errors'] += 1

        return result


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
                log_error(e)
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
