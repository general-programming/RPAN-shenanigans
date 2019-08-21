# Load sentry before modules
import sentry_sdk
import logging
import os
if "SENTRY_DSN" in os.environ:
    from sentry_sdk.integrations.logging import LoggingIntegration
    from sentry_sdk.integrations.excepthook import ExcepthookIntegration
    from sentry_sdk.integrations.stdlib import StdlibIntegration
    from sentry_sdk.integrations.threading import ThreadingIntegration
    sentry_sdk.init(os.environ["SENTRY_DSN"], integrations=[
         LoggingIntegration(),
         ExcepthookIntegration(always_run=True),
         StdlibIntegration(),
        ThreadingIntegration()
    ])

import uvloop

# Logging configuration
if "DEBUG_FLOOD" in os.environ:
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("rpandumper").setLevel(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

# asyncio debug isn't that bad
logging.getLogger("asyncio").setLevel(logging.DEBUG)
logging.getLogger("rpandumper").setLevel(level=logging.DEBUG)

# Install uvloop globally
uvloop.install()
print("nodejs loop installed")
