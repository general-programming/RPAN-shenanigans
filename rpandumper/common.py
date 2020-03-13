import praw
import os
import time
import sys
import asyncio
import json
import logging
import redis
import random

from collections import defaultdict

logger = logging.getLogger("rpandumper.common")

# Common constants
STREAMS_BASE = os.environ["STREAMS_BASE"]

# Utility helpers
def create_praw() -> praw.Reddit:
    extra_args = {}

    if "REDDIT_USERNAME" in os.environ and "REDDIT_PASSWORD" in os.environ:
        extra_args["username"] = os.environ["REDDIT_USERNAME"]
        extra_args["password"] = os.environ["REDDIT_PASSWORD"]
        print("Got username/pw")

    return praw.Reddit(
        client_id=os.environ["REDDIT_PUBLIC"],
        client_secret=os.environ["REDDIT_SECRET"],
        redirect_uri='http://localhost:8080',
        user_agent='RPAN scraper by u/nepeat',
        **extra_args
    )

def create_redis() -> redis.StrictRedis:
    return redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://localhost"))

# TODO: Explain what is this magical dataclass.
class IngestItem:
    __slots__ = ["tag", "time", "data"]
    def __init__(self, tag: str, data: str):
        self.tag = tag
        self.data = data

        self.time = int(time.time())

# Common code shared between HLS, socket dumper, and other future workers.
class BaseWorker:
    def __init__(self, loop: asyncio.BaseEventLoop):
        self.loop = loop
        self.loop.set_exception_handler(self.loop_error)
        self.running = True
        self.tasks = []
        self.extra_tasks = []
        self.reddit = create_praw()

        self.science = defaultdict(lambda: 0)

    @property
    def reddit_headers(self):
        if not self.reddit._core._authorizer.access_token:
            self.reddit.user.me(False)

        return {
            "Authorization": "Bearer " + self.reddit._core._authorizer.access_token
        }

    def loop_error(self, loop, context):
        """Ignore aiohttp #3535 / cpython #13548 issue with SSL data after close

        There is an issue in Python 3.7 up to 3.7.3 that over-reports a
        ssl.SSLError fatal error (ssl.SSLError: [SSL: KRB5_S_INIT] application data
        after close notify (_ssl.c:2609)) after we are already done with the
        connection. See GitHub issues aio-libs/aiohttp#3535 and
        python/cpython#13548.

        Given a loop, this sets up an exception handler that ignores this specific
        exception, but passes everything else on to the previous exception handler
        this one replaces.

        Checks for fixed Python versions, disabling itself when running on 3.7.4+
        or 3.8.

        """
        if sys.version_info <= (3, 7, 4):
            if context.get("message") in {
                "SSL error in data received",
                "Fatal error on transport",
            }:
                # validate we have the right exception, transport and protocol
                exception = context.get('exception')
                protocol = context.get('protocol')
                if (
                    isinstance(exception, ssl.SSLError)
                    and exception.reason == 'KRB5_S_INIT'
                    and isinstance(protocol, SSL_PROTOCOLS)
                ):
                    if loop.get_debug():
                        asyncio.log.logger.debug('Ignoring asyncio SSL KRB5_S_INIT error')
                    return

        print(context)

    # General seed scraper
    async def scrape_seed(self):
        while self.running:
            # Coverage for both videos and broadcasts endpoints.
            endpoint = random.choice(["videos", "broadcasts"])
            await asyncio.sleep(2)
            async with self.session.get(
                "https://strapi.reddit.com/" + endpoint,
                # headers=self.reddit_headers,
                allow_redirects=True,
            ) as response:
                self.science_incr("seed_fetch")

                # Parse data JSON.
                _data = await response.text()
                if hasattr(self, "ingest_queue"):
                    self.ingest_queue.put(IngestItem("seed_response", _data))

                try:
                    data = json.loads(_data)
                except json.JSONDecodeError:
                    logger.warning("Couldn't parse seed data. Status code %d.", response.status)
                    logger.debug(_data)
                    continue

                # Save data and parse.
                seed_data = data.get("data", [])
                if not seed_data:
                    logger.debug("Seed data is empty?")
                    logger.debug(data)
                    self.science_incr("empty_seed")
                    continue

                await self.parse_seed(seed_data)

    # General background tasks
    async def refresh_reddit_auth(self):
        while self.running:
            self.reddit.user.me(False)
            await asyncio.sleep(120)
            self.science_incr("auth_refresh")

    # Science
    """
        Handles updating rpan:science:XYZ keys.
        Implementation is worker dependent.

        XXX: wow, what a cursed name
    """
    def thanos_cock(self):
        raise NotImplementedError

    def science_incr(self, key):
        self.science[key] += 1

    # Runtime handling
    def run(self):
        try:
            self.loop.run_until_complete(self._run())
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt caught, cleaning up?")
            self.loop.run_until_complete(self.cleanup())

    async def _run(self):
        self.redis = await self.redis

        # Default general tasks
        self.tasks.append(asyncio.create_task(self.scrape_seed()))
        self.tasks.append(asyncio.create_task(self.thanos_cock()))
        self.tasks.append(asyncio.create_task(self.refresh_reddit_auth()))

        # Worker specific tasks
        for task in self.extra_tasks:
            self.tasks.append(self.loop.create_task(task()))

        # Hold until all tasks are done.
        await asyncio.gather(*self.tasks)

    async def cleanup(self):
        self.running = False
        self.redis.close()
        await self.redis.wait_closed()

        # XXX: Figure out what to do with stray tasks.
        # for task in self.tasks:
        #     task.cancel()