from pathlib import Path
from typing import AsyncGenerator, List

import aiofiles
import ijson
from cpe import CPE
from lxml import etree

from .logger import LogManager
from .models import CPECreate

logger = LogManager('parser.py')

CPE_NAMESPACE = {"cpe23": "http://scap.nist.gov/schema/cpe-extension/2.3"}


async def parse_xml_in_batches(xml_path: Path, batch_size: int = 2_000) -> AsyncGenerator[List[CPECreate], None]:
    try:
        batch = []
        context = etree.iterparse(str(xml_path), events=("end",), tag="{http://cpe.mitre.org/dictionary/2.0}cpe-item")

        for event, elem in context:
            cpe23_item = elem.find("cpe23:cpe23-item", namespaces=CPE_NAMESPACE)
            if cpe23_item is None:
                continue

            cpe23Uri = cpe23_item.get("name")
            if not cpe23Uri:
                continue

            try:
                cpe_obj = CPE(cpe23Uri)
            except Exception as e:
                logger.error(f"Error parsing CPE URI {cpe23Uri}: {str(e)}")
                continue

            cpe_type = (
                "software" if cpe_obj.is_application() else
                "operating_system" if cpe_obj.is_operating_system() else
                "hardware" if cpe_obj.is_hardware() else
                "other"
            )

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
                cpe_name=cpe_obj.as_fs(),
                type=cpe_type,
            )

            batch.append(cpe)
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            if len(batch) >= batch_size:
                logger.info(f"Yielding batch of {len(batch)} CPEs.")
                yield batch
                batch = []

        if batch:
            logger.info(f"Yielding final batch of {len(batch)} CPEs.")
            yield batch

        logger.info("XML streaming parsing completed.")
    except Exception as e:
        logger.error(f"Error while streaming XML: {str(e)}")


async def parse_cpes_from_cve_json_in_batches(json_path: Path, batch_size: int = 2_000) -> AsyncGenerator[
    List[CPECreate], None]:
    try:
        batch = []
        async with aiofiles.open(json_path, "rb") as json_file:
            async for cve_item in ijson.items(json_file, "CVE_Items.item"):
                configurations = cve_item.get("configurations", {})
                nodes = configurations.get("nodes", [])

                for node in nodes:
                    cpe_matches = node.get("cpe_match", [])

                    for match in cpe_matches:
                        cpe23Uri = match.get('cpe23Uri')
                        if not cpe23Uri:
                            continue

                        try:
                            cpe_obj = CPE(cpe23Uri)
                        except Exception as e:
                            logger.error(f"Error parsing CPE URI {cpe23Uri}: {str(e)}")
                            continue

                        cpe_type = (
                            "software" if cpe_obj.is_application() else
                            "operating_system" if cpe_obj.is_operating_system() else
                            "hardware" if cpe_obj.is_hardware() else
                            "other"
                        )

                        cpe = CPECreate(
                            cpe_name=cpe_obj.as_fs(),
                            type=cpe_type,
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
                            other=cpe_obj.get_other()[0]
                        )

                        batch.append(cpe)

                        if len(batch) >= batch_size:
                            yield batch
                            batch = []

        if batch:
            yield batch

        logger.info("CPE parsing from CVE JSON completed.")
    except Exception as e:
        logger.error(f"Error while streaming JSON for CPEs: {str(e)}")
