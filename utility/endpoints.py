import asyncio
import os
from pathlib import Path

import markdown2
from dotenv import load_dotenv
from fastapi import APIRouter, BackgroundTasks, Depends
from fastapi.responses import HTMLResponse, JSONResponse

from .auth import authenticate
from .config import settings
from .crud import reset_stats, get_stats, record_stats, read_version_file, \
    read_markdown_file, get_cpe, bulk_create_or_update_cpes
from .downloader import download_file
from .extractor import extract_zip
from .health_check import check_mongo, check_kafka, check_url, check_internet_connection, check_loki
from .logger import LogManager
from .parser import parse_json_in_batches, parse_cpes_from_cve_json_in_batches

load_dotenv()

logger = LogManager('endpoints.py')

router = APIRouter()

VERSION_FILE_PATH = Path(__file__).parent.parent / 'version.txt'
README_FILE_PATH = Path(__file__).parent.parent / 'README.md'


async def download_and_extract(url: str, zip_path: Path, extract_to: Path) -> Path:
    # try:
        await reset_stats()
        logger.info("Starting download and extraction process.")
        await download_file(url, zip_path)
        logger.info("Download completed.")
        await extract_zip(zip_path, extract_to)
        logger.info("Extraction completed.")
        return extract_to / zip_path.stem
    # except Exception as e:
    #     logger.error(f"Error during download and extraction: {str(e)}")
    #     raise  # Re-raise the exception to handle it at a higher level


async def process_cpes_in_batches(json_file_path: Path):
    try:
        logger.info("Starting JSON processing.")

        # Define a semaphore to control the concurrency limit
        semaphore = asyncio.Semaphore(10)  # Adjust the concurrency as needed

        async def process_batch(batch):
            async with semaphore:
                await bulk_create_or_update_cpes(batch)

        # Process batches of CPEs
        async for batch in parse_json_in_batches(json_file_path):
            logger.info(f"Processing batch of size: {len(batch)}")
            await process_batch(batch)

        logger.info("Finished processing all CPE batches.")
    except Exception as e:
        logger.error(f"Error during processing CPEs in batches: {str(e)}")
        raise  # Re-raise the exception to handle it at a higher level


@record_stats()
async def update_cpes():
    try:
        base_dir = Path(settings.FILES_BASE_DIR) / 'downloaded'
        extract_to = base_dir / 'extracted_files'
        logger.info("Starting CPE update.")

        url = settings.CPE_URL
        zip_path = base_dir / 'nvdcpematch-1.0.json.zip'
        json_file_path = await download_and_extract(url, zip_path, extract_to)

        logger.info(f"JSON file path: {json_file_path}")

        await process_cpes_in_batches(json_file_path)
        logger.info("CPE update completed.")
    except Exception as e:
        logger.error(f"Error during CPE update: {str(e)}")
        raise  # Re-raise the exception to handle it at a higher level


async def process_recent_cpes_in_batches(json_file_path: Path):
    try:
        logger.info("Starting JSON processing.")

        # Define a semaphore to control the concurrency limit
        semaphore = asyncio.Semaphore(10)  # Adjust the concurrency as needed

        async def process_batch(batch):
            async with semaphore:
                await bulk_create_or_update_cpes(batch)

        # Process batches of CPEs
        async for batch in parse_cpes_from_cve_json_in_batches(json_file_path):
            print('*********************',len(batch))
            logger.info(f"Processing batch of size: {len(batch)}")
            await process_batch(batch)

        logger.info("Finished processing all CPE batches.")
    except Exception as e:
        logger.error(f"Error during processing CPEs in batches: {str(e)}")
        raise  # Re-raise the exception to handle it at a higher level


@record_stats()
async def update_recent_cpes(feed_type: str):
    try:
        base_dir = Path(settings.FILES_BASE_DIR) / 'downloaded'
        extract_to = base_dir / 'extracted_files'
        logger.info("Starting CPE update.")

        url = getattr(settings, f"CPE_{feed_type.upper()}_URL")
        zip_path = base_dir / f'nvdcve-1.1-{feed_type}.json.zip'
        json_file_path = await download_and_extract(url, zip_path, extract_to)

        logger.info(f"JSON file path: {json_file_path}")

        await process_recent_cpes_in_batches(json_file_path)
        logger.info("CPE update completed.")
    except Exception as e:
        logger.error(f"Error during CPE update: {str(e)}")
        raise  # Re-raise the exception to handle it at a higher level


@router.post("/all")
async def update_cpes_endpoint(background_tasks: BackgroundTasks, token: str, username: str = Depends(authenticate)):
    if not token == os.getenv('VERIFICATION_TOKEN'):
        return {"error": "Invalid request"}

    try:
        logger.info("Received update CPE request.")
        background_tasks.add_task(update_cpes)  # Run this in the background
        return {"message": 'Started updating CPEs in the background!'}
    except Exception as e:
        logger.error(f"Error in update_cpes_endpoint: {str(e)}")
        return {"error": "Failed to start CPE update."}


@router.post("/recent")
async def update_recent_cpes_endpoint(background_tasks: BackgroundTasks, token: str,
                                      username: str = Depends(authenticate)):
    if not token == os.getenv('VERIFICATION_TOKEN'):
        return {"error": "Invalid request"}

    try:
        logger.info("Received update CPE request.")
        background_tasks.add_task(update_recent_cpes, "recent")  # Run this in the background
        background_tasks.add_task(update_recent_cpes, "modified")
        return {"message": 'Started updating CPEs in the background!'}
    except Exception as e:
        logger.error(f"Error in update_cpes_endpoint: {str(e)}")
        return {"error": "Failed to start CPE update."}


@router.get("/stats")
async def get_cpes_stats(username: str = Depends(authenticate)):
    return await get_stats()


@router.get("/health_check")
async def check_health(username: str = Depends(authenticate)):
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
async def get_version(username: str = Depends(authenticate)):
    try:
        version = await read_version_file(VERSION_FILE_PATH)
        return {"version": version}
    except FileNotFoundError as e:
        logger.error(e)
    except Exception as e:
        logger.error(e)


@router.get("/readme", response_class=HTMLResponse)
async def get_readme(username: str = Depends(authenticate)):
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
async def get_detail(cpe_name: str, username: str = Depends(authenticate)):
    cpe = await get_cpe(cpe_name)
    if not cpe:
        return JSONResponse(status_code=404, content={"message": f'{cpe_name} not found'})
    return cpe
