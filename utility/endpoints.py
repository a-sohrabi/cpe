import asyncio
from pathlib import Path

import markdown2
from fastapi import APIRouter, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse

from .config import settings
from .crud import create_or_update_cpe, reset_stats, get_stats, record_stats, read_version_file, \
    read_markdown_file, get_cpe
from .downloader import download_file
from .extractor import extract_zip
from .health_check import check_mongo, check_kafka, check_url, check_internet_connection, check_loki
from .logger import log_error
from .parser import parse_xml  # Adjust parser function as needed for XML

router = APIRouter()

VERSION_FILE_PATH = Path(__file__).parent.parent / 'version.txt'
README_FILE_PATH = Path(__file__).parent.parent / 'README.md'


async def download_and_extract(url: str, zip_path: Path, extract_to: Path) -> Path:
    await reset_stats()
    await download_file(url, zip_path)
    await extract_zip(zip_path, extract_to)
    return extract_to / zip_path.stem


async def process_cpes(xml_file_path: Path):
    print(f"Processing CPEs from: {xml_file_path}")
    cpes = await parse_xml(xml_file_path)
    if not cpes:
        print("No CPEs found.")
    else:
        for cpe in cpes:
            print(f"Parsed CPE: {cpe}")
        tasks = [create_or_update_cpe(cpe) for cpe in cpes]
        await asyncio.gather(*tasks)


@record_stats()
async def update_cpes():
    base_dir = Path(settings.FILES_BASE_DIR) / 'downloaded'
    extract_to = base_dir / 'extracted_files'

    url = settings.CPE_URL
    zip_path = base_dir / 'official-cpe-dictionary_v2.3.xml.zip'
    xml_file_path = await download_and_extract(url, zip_path, extract_to)
    await process_cpes(xml_file_path)


@router.get("/update")
async def update_cpes_endpoint(background_tasks: BackgroundTasks):
    await update_cpes()
    return {"message": 'Started updating CPEs in the background!'}


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
        log_error(e, {'function': 'get_version', 'context': 'file not found'})
    except Exception as e:
        log_error(e, {'function': 'get_version', 'context': 'other exceptions'})


@router.get("/readme", response_class=HTMLResponse)
async def get_readme():
    try:
        content = await read_markdown_file(README_FILE_PATH)
        html_content = markdown2.markdown(content)
        return HTMLResponse(content=html_content, headers={"Content-Type": "text/markdown; charset=utf-8"},
                            status_code=200)
    except FileNotFoundError as e:
        log_error(e, {"function": "get_readme", "context": "file not found"})
        return JSONResponse(status_code=404, content={"message": "File not found"})
    except Exception as e:
        log_error(e)


@router.get('/detail/{cpe_id}')
async def get_detail(cpe_id: str):
    cpe = await get_cpe(cpe_id)
    if not cpe:
        return JSONResponse(status_code=404, content={"message": f'{cpe_id} not found'})
    return cpe
