import logging
import os

# Logging configuration
if "DEBUG_FLOOD" in os.environ:
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("rpandumper").setLevel(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

# asyncio debug isn't that bad
logging.getLogger("asyncio").setLevel(logging.DEBUG)
logging.getLogger("rpandumper").setLevel(level=logging.DEBUG)

