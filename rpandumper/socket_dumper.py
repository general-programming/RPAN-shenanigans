import logging
import asyncio
import os
import json
import queue
import time
import uuid
import ssl
import sys
import datetime
import traceback

from collections import defaultdict

import aiohttp
import aioredis

from rpandumper.common import create_praw

logger = logging.getLogger("rpandumper")

# XXX HACK LOL
logger.setLevel(level=logging.DEBUG)
ALL_SCOPES = ['creddits', 'modcontributors', 'modmail', 'modconfig', 'subscribe', 'structuredstyles', 'vote', 'wikiedit', 'mysubreddits', 'submit', 'modlog', 'modposts', 'modflair', 'save', 'modothers', 'read', 'privatemessages', 'report', 'identity', 'livemanage', 'account', 'modtraffic', 'wikiread', 'edit', 'modwiki', 'modself', 'history', 'flair']

class IngestItem:
    __slots__ = ["tag", "time", "data"]
    def __init__(self, tag: str, data: str):
        self.tag = tag
        self.data = data

        self.time = int(time.time())

SSL_PROTOCOLS = (asyncio.sslproto.SSLProtocol,)
try:
    import uvloop.loop
except ImportError:
    pass
else:
    SSL_PROTOCOLS = (*SSL_PROTOCOLS, uvloop.loop.SSLProtocol)

def ignore_aiohttp_ssl_eror(loop):
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
    if sys.version_info >= (3, 7, 4):
        return

    orig_handler = loop.get_exception_handler()

    def ignore_ssl_error(loop, context):
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
        if orig_handler is not None:
            orig_handler(loop, context)
        else:
            loop.default_exception_handler(context)

    loop.set_exception_handler(ignore_ssl_error)

class Dumper:
    def __init__(self, loop: asyncio.BaseEventLoop):
        self.loop = loop
        self.loop.set_exception_handler(self.loop_error)
        ignore_aiohttp_ssl_eror(loop)
        self.running = True
        self.tasks = []
        self.ingest_queue = queue.Queue()
        self.sockets_ingesting = {}

        self.reddit = create_praw()
        self.redis = aioredis.create_redis_pool(
            os.environ.get("REDIS_URL", "redis://localhost"),
            minsize=5,
            maxsize=69,  # hehe 69
            loop=loop,
        )

        # Setup HTTP session
        conn = aiohttp.TCPConnector(limit=0)  # Uncap the max HTTP connections.
        self.session = aiohttp.ClientSession(
            loop=loop,
            headers={
                "User-Agent": "RPAN dumper sponsored by u/nepeat"
            },
            connector=conn,
        )

    def loop_error(self, loop, context):
        print(context)

    # Ingest / Utility
    async def refresh_reddit_auth(self):
        while self.running:
            self.reddit.user.me(False)
            await asyncio.sleep(120)

    async def process_queues(self):
        while self.running:
            await asyncio.sleep(0.25)
            while not self.ingest_queue.empty():
                item = self.ingest_queue.get()
                await self.redis.zadd("rpan:events:" + item.tag, item.time, item.data)

    @property
    def reddit_headers(self):
        if not self.reddit._core._authorizer.access_token:
            self.reddit.user.me(False)

        return {
            "Authorization": "Bearer " + self.reddit._core._authorizer.access_token
        }

    async def do_heartbeats(self):
        # https://strapi.reddit.com/videos/t3_ct2etl/heartbeat
        while self.running:
            # for name in self.sockets_ingesting.keys():
            #     if not name.startswith("comments_"):
            #         continue
                
            #     post = name.lstrip("comments_")
            #     async with self.session.post(f"https://oauth.reddit.com/videos/{post}/heartbeat", headers=self.reddit_headers) as req:
            #         print(await req.text())
            await asyncio.sleep(5)

    # Seed related code
    async def scrape_seed(self):
        while self.running:
            await asyncio.sleep(5)
            async with self.session.get("https://strapi.reddit.com/videos/seed", allow_redirects=True) as response:
                # Parse data JSON.
                _data = await response.text()
                self.ingest_queue.put(IngestItem("seed_response", _data))

                try:
                    data = json.loads(_data)
                except json.JSONDecodeError:
                    logger.warning("Couldn't parse seed data. Status code %d.", response.status)
                    logging.debug(_data)
                    continue

                # Save data and parse.
                seed_data = data.get("data", [])
                if not seed_data:
                    logging.debug("Seed data is empty?")
                    logging.debug(data)
                await self.parse_seed(seed_data)

    async def parse_seed(self, seed_data: dict):
        # code that saves api response to redis
        for stream in seed_data:
            updates_websocket = stream.get("updates_websocket", None)
            post_data = stream.get("post", {"id": "unknown_" + str(uuid.uuid4())})
            stream_data = stream.get("stream", {})

            # Do something with the updates websocket
            # if updates_websocket:
            #     await self.vore_socket("updates_" + post_data["id"], updates_websocket)

            # Do something with the comments websocket
            if post_data:
                live_comments_websocket = post_data.get("liveCommentsWebsocket", None)
                if live_comments_websocket:
                    # DIRTY HACK
                    live_comments_websocket = live_comments_websocket.replace("reddit.com", "ws-05b875714591d37f6.wss.redditmedia.com")
                    await self.vore_socket("comments_" + post_data["id"], live_comments_websocket)

    # Websocket ingest code
    async def vore_socket(self, socket_id: str, socket_url: str):
        if socket_id in self.sockets_ingesting:
            if not self.sockets_ingesting[socket_id].done():
                return

        self.sockets_ingesting[socket_id] = asyncio.create_task(self._vore_socket(socket_id, socket_url))

    async def _vore_socket(self, socket_id: str, socket_url: str):
        connected = True
        failures = 0

        if not socket_url:
            logger.debug("bogon")
            return

        while self.running and connected:
            try:
                print(socket_id, socket_url)
                async with self.session.ws_connect(socket_url) as ws:
                    msg = await ws.receive()

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self.ingest_queue.put(IngestItem("socket:" + socket_id, msg.data))
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logging.debug("Stream %s errored.", socket_id)
                    elif msg.type == aiohttp.WSMsgType.CLOSING:
                        logging.warning("Stream %s is closing on the other end.", socket_id)

                    connected = False
                    logging.debug("Stream %s peacefully closed.", socket_id)
            except aiohttp.WSServerHandshakeError as e:
                print("Stream %s failed to connect. Status code %s Message '%s'" % (socket_id, e.code, e.message), flush=True)
                await asyncio.sleep((failures * 0.5) + 1)
                failures += 1
                if failures > 3:
                    connected = False

        logger.debug(datetime.datetime.now().isoformat() + socket_id + " has died")

    # Management code
    async def _run(self):
        self.redis = await self.redis

        # Tasks
        self.tasks.append(asyncio.create_task(self.refresh_reddit_auth()))
        self.tasks.append(asyncio.create_task(self.process_queues()))
        self.tasks.append(asyncio.create_task(self.scrape_seed()))
        self.tasks.append(asyncio.create_task(self.do_heartbeats()))

        # Hold until all tasks are done.
        await asyncio.gather(*self.tasks)

    async def cleanup(self):
        self.running = False
        self.redis.close()
        await self.redis.wait_closed()
        # for task in self.tasks:
        #     task.cancel()

    def run(self):
        try:
            self.loop.run_until_complete(self._run())
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt caught, cleaning up?")
            self.loop.run_until_complete(self.cleanup())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    dumper = Dumper(loop)
    dumper.run()