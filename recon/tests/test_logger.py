# Demo to create my own log handler

import logging
import sys
import xml.etree.ElementTree as ET
import xml.dom.minidom

doc = ET.Element('dfxml')
log = ET.SubElement(doc, 'log')

class DFXMLLoggingHandler(logging.Handler):
    IGNORE_ATTRS = set(['getMessage'])
    def emit(self,record):
        attrs = {attr:str(getattr(record,attr))
                 for attr in dir(record)
                 if not attr.startswith("__") and attr not in self.IGNORE_ATTRS}
        ET.SubElement(log, 'record', attrs)

def foo():
    logger.error('error within foo')

if __name__=="__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    dh = DFXMLLoggingHandler()
    logger.addHandler(dh)

    h2 = logging.StreamHandler(sys.stdout)
    h2.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    h2.setFormatter(formatter)
    logger.addHandler(h2)


    logger.info('a test at info')
    logger.warning('a test at warn')
    foo()

    print(xml.dom.minidom.parseString(ET.tostring(doc).decode('utf-8')).toprettyxml(indent='  '))
