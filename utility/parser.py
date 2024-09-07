from pathlib import Path
from typing import AsyncGenerator, List

import aiofiles
import ijson

from .logger import LogManager
from .models import CPECreate

from cpe import CPE

logger = LogManager('parser.py')


async def parse_json_in_batches(json_path: Path, batch_size: int = 2_000) -> AsyncGenerator[List[CPECreate], None]:
    try:
        batch = []
        async with aiofiles.open(json_path, "rb") as json_file:
            async for match in ijson.items(json_file, "matches.item"):
                cpe23Uri = match.get('cpe23Uri')
                if not cpe23Uri:
                    continue

                try:
                    cpe_obj = CPE(cpe23Uri)
                except Exception as e:
                    logger.error(f"Error parsing CPE URI {cpe23Uri}: {str(e)}")
                    continue

                if cpe_obj.is_application():
                    cpe_type = "software"
                elif cpe_obj.is_operating_system():
                    cpe_type = "operating_system"
                elif cpe_obj.is_hardware():
                    cpe_type = "hardware"
                else:
                    cpe_type = 'other'

                cpe = CPECreate(
                    cpe_version=cpe_obj.get_version()[0],
                    part=cpe_obj.get_part()[0],
                    vendor=cpe_obj.get_vendor()[0],
                    product=cpe_obj.get_product()[0],
                    version=cpe_obj.get_version()[0],
                    update=cpe_obj.get_update()[0],
                    edition=cpe_obj.get_edition()[0],
                    language=cpe_obj.get_language()[0],
                    sw_edition=cpe_obj.get_software_edition()[0],
                    target_sw=cpe_obj.get_target_software()[0],
                    target_hw=cpe_obj.get_target_hardware()[0],
                    other=cpe_obj.get_other()[0],
                    cpe_id=cpe_obj.as_uri_2_3(),  # Use the full URI as the unique ID
                    type=cpe_type
                )

                batch.append(cpe)

                if len(batch) >= batch_size:
                    yield batch
                    batch = []

        if batch:
            yield batch

        logger.info("JSON streaming parsing completed.")
    except Exception as e:
        logger.error(f"Error while streaming JSON: {str(e)}")
