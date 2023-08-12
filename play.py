import base64
import logging
import time

import psycopg2

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s:%(message)s')
new_block_uuid=''
logging.error(">>>%s<<<", base64.standard_b64encode(str(new_block_uuid).encode()).decode())