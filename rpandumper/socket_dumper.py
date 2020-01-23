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

import aiohttp
import aioredis

from rpandumper.common import IngestItem, BaseWorker

logger = logging.getLogger("rpandumper.socketdumper")

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

class Dumper(BaseWorker):
    def __init__(self, loop: asyncio.BaseEventLoop):
        super().__init__(loop)

        # Socket dumper queue dumper task.
        self.extra_tasks.append(self.process_queues)

        self.ingest_queue = queue.Queue()
        self.sockets_ingesting = {}
        self.active_rooms = set()

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

    # Ingest / Utility
    async def process_queues(self):
        while self.running:
            await asyncio.sleep(0.1)
            while not self.ingest_queue.empty():
                item = self.ingest_queue.get()
                await self.redis.zadd("rpan:events:" + item.tag, item.time, item.data)
                await self.redis.publish("rpan:events:" + item.tag, item.data)
                self.science_incr("event_processed")

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

            # Sleep for 0.5 seconds.
            await asyncio.sleep(0.5)

    # Seed parser code
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
                    # 01/23/2020 Seems reddit is returning sane redditmedia URLs now. Pls nuke later.
                    # wsdomain = random.choice([
                    #     "00b2ec7e0811b4d7a",
                    #     "05b875714591d37f6",
                    #     "093e8f4e67ed67e08",
                    #     "021face97bba27158",
                    # ])
                    # live_comments_websocket = live_comments_websocket.replace("reddit.com", f"ws-{wsdomain}.wss.redditmedia.com")

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

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    dumper = Dumper(loop)
    dumper.run()
