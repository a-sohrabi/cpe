from pathlib import Path
from typing import List

from lxml import etree

from .logger import logger, log_error
from .schemas import CPECreate


async def parse_xml(xml_path: Path) -> List[CPECreate]:
    try:
        context = etree.iterparse(str(xml_path), tag="{http://cpe.mitre.org/dictionary/2.0}cpe-item", events=('end',))
        cpes = []
        for _, elem in context:
            cpe_id = elem.find("{http://scap.nist.gov/schema/cpe-extension/2.3}cpe23-item").get("name")
            title = elem.find("{http://cpe.mitre.org/dictionary/2.0}title").text
            references = [ref.text for ref in elem.findall("{http://cpe.mitre.org/dictionary/2.0}reference")]
            deprecated_element = elem.find("{http://scap.nist.gov/schema/cpe-extension/2.3}deprecated")
            deprecated = deprecated_element is not None and deprecated_element.text == "true"
            deprecated_by = elem.find(
                "{http://scap.nist.gov/schema/cpe-extension/2.3}deprecated-by").text if deprecated else None

            cpes.append(CPECreate(
                cpe_id=cpe_id,
                title=title,
                references=references,
                deprecated=deprecated,
                deprecated_by=deprecated_by,
            ))

            # Clear the element from memory
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        logger.info("XML parsing completed")
        return cpes
    except Exception as e:
        log_error(e, {'function': 'parse_xml', 'context': 'parsing the xml of CPEs'})
        return []
