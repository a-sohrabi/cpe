import xml.etree.ElementTree as ET
from pathlib import Path
from typing import List

from .logger import logger, log_error
from .schemas import CPECreate

# Define the namespaces used in the XML
NAMESPACE = "http://cpe.mitre.org/dictionary/2.0"
CPE23_NAMESPACE = "http://scap.nist.gov/schema/cpe-extension/2.3"

# A dictionary to hold the namespaces
NAMESPACES = {
    'cpe': NAMESPACE,
    'cpe23': CPE23_NAMESPACE
}


async def parse_xml(xml_path: Path) -> List[CPECreate]:
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        cpes = []
        # Find all cpe-item elements using the default namespace
        for item in root.findall("cpe:cpe-item", NAMESPACES):
            # CPE 2.3 item
            cpe_id_element = item.find("cpe23:cpe23-item", NAMESPACES)
            cpe_id = cpe_id_element.get("name") if cpe_id_element is not None else None

            # Title
            title_element = item.find("cpe:title", NAMESPACES)
            title = title_element.text if title_element is not None else None

            # References
            references = [ref.text for ref in item.findall("cpe:references/cpe:reference", NAMESPACES)]

            # Deprecated status
            deprecated_element = item.find("cpe23:deprecated", NAMESPACES)
            deprecated = deprecated_element.text == "true" if deprecated_element is not None else False
            deprecated_by_element = item.find("cpe23:deprecated-by", NAMESPACES)
            deprecated_by = deprecated_by_element.text if deprecated_by_element is not None else None

            # Add to the list
            cpes.append(CPECreate(
                cpe_id=cpe_id,
                title=title,
                references=references,
                deprecated=deprecated,
                deprecated_by=deprecated_by,
            ))

        logger.info("XML parsing completed")
        return cpes
    except Exception as e:
        log_error(e, {'function': 'parse_xml', 'context': 'parsing the xml of CPEs'})
        return []
