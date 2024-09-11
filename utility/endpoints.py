import asyncio
import os
from pathlib import Path

import markdown2
from dotenv import load_dotenv
from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse

from .config import settings
from .crud import reset_stats, get_stats, record_stats, read_version_file, \
    read_markdown_file, get_cpe, bulk_create_or_update_cpes
from .downloader import download_file
from .extractor import extract_zip
from .health_check import check_mongo, check_kafka, check_url, check_internet_connection, check_loki
from .logger import LogManager
from .parser import parse_cpes_from_cve_json_in_batches, parse_xml_in_batches

load_dotenv()

logger = LogManager('endpoints.py')

router = APIRouter()

VERSION_FILE_PATH = Path(__file__).parent.parent / 'version.txt'
README_FILE_PATH = Path(__file__).parent.parent / 'README.md'


async def download_and_extract(url: str, zip_path: Path, extract_to: Path) -> Path:
    await reset_stats()
    await download_file(url, zip_path)
    await extract_zip(zip_path, extract_to)
    return extract_to / zip_path.stem


async def process_cpes_in_batches(json_file_path: Path):
    try:
        semaphore = asyncio.Semaphore(10)

        async def process_batch(batch):
            async with semaphore:
                await bulk_create_or_update_cpes(batch)

        async for batch in parse_xml_in_batches(json_file_path):
            await process_batch(batch)

    except Exception as e:
        logger.error(f"Error during processing CPEs in batches: {str(e)}")


@record_stats()
async def update_cpes():
    try:
        base_dir = Path(settings.FILES_BASE_DIR) / 'downloaded'
        extract_to = base_dir / 'extracted_files'

        url = settings.CPE_URL
        zip_path = base_dir / 'official-cpe-dictionary_v2.3.xml.zip'

        json_file_path = await download_and_extract(url, zip_path, extract_to)

        # Process batches from the XML file
        await process_cpes_in_batches(json_file_path)
        logger.info("Getting all CPEs completed.")
    except Exception as e:
        logger.error(f"Error during getting all CPEs: {str(e)}")


async def process_recent_cpes_in_batches(json_file_path: Path):
    try:
        semaphore = asyncio.Semaphore(10)

        async def process_batch(batch):
            async with semaphore:
                await bulk_create_or_update_cpes(batch)

        async for batch in parse_cpes_from_cve_json_in_batches(json_file_path):
            await process_batch(batch)
    except Exception as e:
        logger.error(f"Error during processing CPEs in batches: {str(e)}")


@record_stats()
async def update_recent_cpes(feed_type: str):
    try:
        base_dir = Path(settings.FILES_BASE_DIR) / 'downloaded'
        extract_to = base_dir / 'extracted_files'

        url = getattr(settings, f"CPE_{feed_type.upper()}_URL")
        zip_path = base_dir / f'nvdcve-1.1-{feed_type}.json.zip'
        json_file_path = await download_and_extract(url, zip_path, extract_to)

        await process_recent_cpes_in_batches(json_file_path)
        logger.info("Getting recent and modified CPEs completed.")
    except Exception as e:
        logger.error(f"Error during getting modified and recent CPEs: {str(e)}")


@router.post("/all")
async def update_cpes_endpoint(background_tasks: BackgroundTasks, token: str):
    if not token == os.getenv('VERIFICATION_TOKEN'):
        return {"error": "Invalid request"}

    try:
        logger.info("Received update CPE request.")
        background_tasks.add_task(update_cpes)
        return {"message": 'Started updating CPEs in the background!'}
    except Exception as e:
        logger.error(f"Error in update_cpes_endpoint: {str(e)}")
        return {"error": "Failed to start CPE update."}


@router.post("/recent")
async def update_recent_cpes_endpoint(background_tasks: BackgroundTasks, token: str):
    if not token == os.getenv('VERIFICATION_TOKEN'):
        return {"error": "Invalid request"}

    try:
        logger.info("Received update CPE request.")
        background_tasks.add_task(update_recent_cpes, "recent")
        background_tasks.add_task(update_recent_cpes, "modified")
        return {"message": 'Started updating CPEs in the background!'}
    except Exception as e:
        logger.error(f"Error in update_recent_CPEs_endpoint: {str(e)}")
        return {"error": "Failed to start CPE update."}


@router.get("/stats")
async def get_cpes_stats():
    return await get_stats()


@router.get("/health_check")
async def check_health():
    mongo_status = await check_mongo()
    kafka_status = await check_kafka()
    cpe_status = await check_url(settings.CPE_URL)
    loki_status = await check_loki()
    internet_status = await check_internet_connection()

    return {
        "internet": internet_status,
        "mongo": mongo_status,
        "kafka": kafka_status,
        "cpe_url": cpe_status,
        "loki": loki_status
    }


@router.get("/version")
async def get_version():
    try:
        version = await read_version_file(VERSION_FILE_PATH)
        return {"version": version}
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        logger.error(e)


@router.get("/readme", response_class=HTMLResponse)
async def get_readme():
    try:
        content = await read_markdown_file(README_FILE_PATH)
        html_content = markdown2.markdown(content)
        return HTMLResponse(content=html_content, headers={"Content-Type": "text/markdown; charset=utf-8"},
                            status_code=200)
    except FileNotFoundError as e:
        logger.error(e)
        return JSONResponse(status_code=404, content={"message": "File not found"})
    except Exception as e:
        logger.error(e)


@router.get('/detail/{cpe_name}')
async def get_detail(cpe_name: str):
    cpe = await get_cpe(cpe_name)
    if not cpe:
        return JSONResponse(status_code=404, content={"message": f'{cpe_name} not found'})
    return cpe
