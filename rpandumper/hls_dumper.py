import logging
import asyncio
import os
import json
import queue
import uuid
import ssl
import datetime
import traceback

from collections import defaultdict

import aiohttp


logger = logging.getLogger("rpandumper")

# XXX HACK LOL
logger.setLevel(level=logging.DEBUG)

class HLSDumper:
    def __init__(self, loop: asyncio.BaseEventLoop):
        self.loop = loop
        self.loop.set_exception_handler(self.loop_error)
        self.running = True
        self.tasks = []
        self.sockets_ingesting = {}

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

    # Seed related code
    async def scrape_seed(self):
        while self.running:
            await asyncio.sleep(5)
            async with self.session.get("https://strapi.reddit.com/videos/seed", allow_redirects=True) as response:
                # Parse data JSON.
                _data = await response.text()

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
        for stream in seed_data:
            post_data = stream.get("post", {"id": "unknown_" + str(uuid.uuid4())})
            stream_data = stream.get("stream", {})

            # Do something with the HLS URL
            if stream_data:
                stream_id = stream_data.get("stream_id", "NOID_" + str(uuid.uuid4()))
                hls_url = stream_data.get("hls_url", None)
                if hls_url:
                    await self.vore_hls(post_data["id"] + "_" + stream_id, hls_url)

    # HLS ingest code
    async def vore_hls(self, stream_id: str, hls_url: str):
        if stream_id in self.sockets_ingesting:
            if not self.sockets_ingesting[stream_id].done():
                return

        self.sockets_ingesting[stream_id] = self.loop.create_task(self._vore_hls(stream_id, hls_url))

    async def _vore_hls(self, stream_id: str, hls_url: str):
        # disgusting hardcoded path
        os.makedirs(f"/srv/rpan/data/streams/{stream_id}", exist_ok=True)

        # ffmpeg -live_start_index 0 -i "$1" /vol/streams/$STREAM_NAME.$(date +%s).ts
        proc = await asyncio.create_subprocess_shell(
            # tasty shell injection
            f'ffmpeg -live_start_index 0 -i "{hls_url}" -c:v libx265 -x265-params crf=23 -c:a copy /srv/rpan/data/streams/{stream_id}/$(date +%s).mkv',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            loop=self.loop
        )

        stdout, stderr = await proc.communicate()
        logger.debug(f'[{stream_id} exited with {proc.returncode}]')

    # Management code
    async def _run(self):
        # Tasks
        self.tasks.append(asyncio.create_task(self.scrape_seed()))

        # Hold until all tasks are done.
        await asyncio.gather(*self.tasks)

    async def cleanup(self):
        self.running = False

    def run(self):
        try:
            self.loop.run_until_complete(self._run())
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt caught, cleaning up?")
            self.loop.run_until_complete(self.cleanup())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    dumper = HLSDumper(loop)
    dumper.run()