import asyncio
import os
import zipfile
from pathlib import Path

from .logger import LogManager

logger = LogManager('extractor.py')


async def extract_zip(zip_path: Path, extract_to: Path):
    try:
        os.makedirs(extract_to, exist_ok=True)
        await asyncio.to_thread(extract_zip_sync, zip_path, extract_to)
    except Exception as e:
        logger.error(e)


def extract_zip_sync(zip_path: Path, extract_to: Path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
