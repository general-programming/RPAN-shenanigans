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
import random

from collections import defaultdict

import aiohttp
import aioredis

from rpandumper.common import create_praw, IngestItem

logger = logging.getLogger("rpandumper")

# XXX HACK LOL
logger.setLevel(level=logging.DEBUG)
ALL_SCOPES = ['creddits', 'modcontributors', 'modmail', 'modconfig', 'subscribe', 'structuredstyles', 'vote', 'wikiedit', 'mysubreddits', 'submit', 'modlog', 'modposts', 'modflair', 'save', 'modothers', 'read', 'privatemessages', 'report', 'identity', 'livemanage', 'account', 'modtraffic', 'wikiread', 'edit', 'modwiki', 'modself', 'history', 'flair']
SSL_PROTOCOLS = (asyncio.sslproto.SSLProtocol,)

try:
    import uvloop.loop
except ImportError:
    pass
else:
    SSL_PROTOCOLS = (*SSL_PROTOCOLS, uvloop.loop.SSLProtocol)

class Dumper:
    def __init__(self, loop: asyncio.BaseEventLoop):
        self.loop = loop
        self.loop.set_exception_handler(self.loop_error)
        self.running = True
        self.tasks = []
        self.ingest_queue = queue.Queue()
        self.sockets_ingesting = {}
        self.active_rooms = set()

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
                "User-Agent": "RPAN socket dumper sponsored by u/nepeat"
            },
            connector=conn,
        )

        self.science = defaultdict(lambda: 0)

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

    # Ingest / Utility
    async def refresh_reddit_auth(self):
        while self.running:
            self.reddit.user.me(False)
            await asyncio.sleep(120)
            self.science_incr("auth_refresh")

    async def process_queues(self):
        while self.running:
            await asyncio.sleep(0.1)
            while not self.ingest_queue.empty():
                item = self.ingest_queue.get()
                await self.redis.zadd("rpan:events:" + item.tag, item.time, item.data)
                await self.redis.publish("rpan:events:" + item.tag, item.data)
                self.science_incr("event_processed")

    @property
    def reddit_headers(self):
        if not self.reddit._core._authorizer.access_token:
            self.reddit.user.me(False)

        return {
            "Authorization": "Bearer " + self.reddit._core._authorizer.access_token
        }

    # Science
    async def thanos_cock(self):
        while self.running:
            # Update socket count
            self.science["socket_count"] = 0

            for task in self.sockets_ingesting.values():
                if task and not task.done():
                    self.science["socket_count"] += 1

            # Unprocessed event count
            self.science["unprocessed_events"] = self.ingest_queue.qsize()

            # Update redis science hash.
            for key, value in self.science.copy().items():
                if key in ("socket_count", "unprocessed_events"):
                    await self.redis.hset("rpan:science:socketdumper", key, value)
                else:
                    await self.redis.hincrby("rpan:science:socketdumper", key, increment=value)
                self.science[key] = 0

            # Sleep for 3 seconds.
            await asyncio.sleep(3)

    def science_incr(self, key):
        self.science[key] += 1

    # Seed related code
    async def scrape_seed(self):
        while self.running:
            await asyncio.sleep(3)
            async with self.session.get("https://strapi.reddit.com/videos/seed", allow_redirects=True) as response:
                self.science_incr("seed_fetch")

                # Parse data JSON.
                _data = await response.text()
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

    async def parse_seed(self, seed_data: dict):
        self.active_rooms.clear()

        # Creates sockets for each room.
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
                    # 08/22/2019 STILL A DIRTY HACK HACK HACK BUT IT WORKS
                    wsdomain = random.choice([
                        "05ba9e4989f78959d",
                        "00b2ec7e0811b4d7a",
                        "05b875714591d37f6",
                        "093e8f4e67ed67e08",
                        "021face97bba27158",
                    ])
                    live_comments_websocket = live_comments_websocket.replace("reddit.com", f"ws-{wsdomain}.wss.redditmedia.com")

                    # Create the socket and yeet off into the sun.
                    room_id = "comments_" + post_data["id"]
                    self.active_rooms.add(room_id)
                    await self.vore_socket(room_id, live_comments_websocket)

    # Websocket ingest code
    async def vore_socket(self, socket_id: str, socket_url: str):
        if socket_id in self.sockets_ingesting:
            if not self.sockets_ingesting[socket_id].done():
                return

        self.sockets_ingesting[socket_id] = asyncio.create_task(self._vore_socket(socket_id, socket_url))
        self.science_incr("socket_opened")

    async def _vore_socket(self, socket_id: str, socket_url: str):
        connected = True
        failures = 0

        if not socket_url:
            logger.error("bogon")
            self.science_incr("socket_bogon")
            return

        # Loop that reconnects connections
        while self.running and connected:
            # Primitive sleep for each failure.
            await asyncio.sleep((failures * 0.5) + .5)

            # Shut the connections attempts down if there are too many failures
            if failures > 3:
                connected = False

            try:
                logger.debug(f"Socket for {socket_id} opened. URL {socket_url}")
                async with self.session.ws_connect(socket_url, timeout=20) as ws:
                    # Loop that recieves messages
                    while self.running and connected:
                        # Loop that checks if the room is alive if we timeout on message receieve.
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=60)
                        except asyncio.TimeoutError:
                            # The room is still active, carry on.
                            if socket_id in self.active_rooms:
                                continue

                            # Yeet the room if it is not active.
                            await ws.close()
                            logger.info("Stream %s has timed out.", socket_id)
                            connected = False
                            self.science_incr("socket_timeout")
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self.ingest_queue.put(IngestItem("socket:" + socket_id, msg.data))
                            self.science_incr("event_queued")
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.debug("Stream %s errored.", socket_id)
                            self.science_incr("socket_error")
                            failures += 1
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSING:
                            logger.warning("Stream %s is closing on the other end.", socket_id)
                            connected = False
                            self.science_incr("socket_closing")
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            logger.warning("Stream %s is closed.", socket_id)
                            connected = False
                            self.science_incr("socket_closed")
                            break
                        else:
                            logger.warning("Unknown message type: %r", msg.type)
                            self.science_incr("socket_unknown")
            except aiohttp.WSServerHandshakeError as e:
                print("Stream %s failed to connect. Status code %s Message '%s'" % (socket_id, e.code, e.message), flush=True)
                failures += 1
                self.science_incr("socket_handshake_fail")

        connected = False
        logger.debug(datetime.datetime.now().isoformat() + socket_id + " has died")
        self.science_incr("socket_task_close")

    # Management code
    async def _run(self):
        self.redis = await self.redis

        # Tasks
        self.tasks.append(asyncio.create_task(self.refresh_reddit_auth()))
        self.tasks.append(asyncio.create_task(self.process_queues()))
        self.tasks.append(asyncio.create_task(self.scrape_seed()))
        self.tasks.append(asyncio.create_task(self.thanos_cock()))

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