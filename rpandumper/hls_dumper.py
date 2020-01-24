import logging
import asyncio
import os
import json
import uuid
import ssl
import datetime
import traceback
import random

import aiohttp
import aioredis

from rpandumper.common import BaseWorker, STREAMS_BASE


logger = logging.getLogger("rpandumper.hlsdumper")

# XXX HACK LOL
logger.setLevel(level=logging.DEBUG)

class HLSDumper(BaseWorker):
    def __init__(self, loop: asyncio.BaseEventLoop):
        super().__init__(loop)
        self.tasks = []
        self.sockets_ingesting = {}

        # Redis
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

    # Science
    async def thanos_cock(self):
        while self.running:
            # Update socket count
            self.science["hls_count"] = 0

            for task in self.sockets_ingesting.values():
                if task and not task.done():
                    self.science["hls_count"] += 1

            # Update redis science hash.
            for key, value in self.science.copy().items():
                if key in ("hls_count"):
                    await self.redis.hset("rpan:science:hlsdumper", key, value)
                else:
                    await self.redis.hincrby("rpan:science:hlsdumper", key, increment=value)
                self.science[key] = 0

            # Sleep for 0.5 seconds.
            await asyncio.sleep(0.5)

    # Seed parser code
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
        self.science_incr("hls_opened")

    async def _vore_hls(self, stream_id: str, hls_url: str):
        # HACK: Very disgusting. It sure beats having more than $MAX 512 byte - 1 GB files on the disk.
        # Drop the stream grab attempt if ffmpeg was already run above a certain magic number.
        # Not a good sign if ffmpeg crashes a lot.
        try:
            grabs = int(await self.redis.hget("rpan:hls:grabs", stream_id))
        except (TypeError, ValueError):
            grabs = 0

        if grabs > 35:
            logger.debug("%s DROPPED: overgrab [%d]", stream_id, grabs)
            return

        # Create the stream folder if it does not exist.
        current_dayfolder = datetime.datetime.utcnow().strftime("raws-%d-%m-%Y")
        stream_folder = os.path.join(STREAMS_BASE, current_dayfolder, stream_id)
        os.makedirs(stream_folder, exist_ok=True)

        # start_index observations:
        #   - We might lose at least a "small" chunk of the start of the video because if the index is 1.
        #   - We start at 0 or 1 since sometimes the 0th index is corrupt due to reasons yet to be known by me.
        #   - Someone more competent will know exactly why this is breaking instead of me using a snake ass oil fix..
        start_index = random.randint(0, 1)
        proc = await asyncio.create_subprocess_shell(
            # tasty shell injection
            f'ffmpeg -live_start_index {start_index} -i "{hls_url}" -c copy -map 0 {stream_folder}/$(date +%s).mkv',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            loop=self.loop
        )

        # Print and log stream grabs.
        stdout, stderr = await proc.communicate()
        self.science_incr("ffmpeg_status_%s" % (proc.returncode))
        logger.debug(f'[{stream_id} exited with {proc.returncode}, {grabs} grabs]')
        await self.redis.hincrby("rpan:hls:grabs", stream_id)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    dumper = HLSDumper(loop)
    dumper.run()