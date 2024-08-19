from pathlib import Path

import aiofiles
import aiohttp

from .logger import log_error


async def download_file(url: str, dest: Path):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()
                content = await response.read()
                async with aiofiles.open(dest, 'wb') as f:
                    await f.write(content)
    except aiohttp.ClientError as e:
        log_error(e, {'function': 'download_file', 'context': 'downloading vulnerability file', 'input': [url, dest]})
