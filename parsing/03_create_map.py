import json

from collections import defaultdict

from rpandumper.model import sm, Stream
from rpandumper.common import create_praw

db = sm()
streams = {}

for stream in db.query(Stream).yield_per(50):
    if not stream.raw_foldername:
        print("LOST STREAM", stream.title)
        continue

    streams[stream.post_id] = {
        "folder": stream.raw_foldername,
        "author": stream.author,
        "title": stream.title,
        "post_id": stream.post_id
    }

json.dump(streams, open("streams.json", "w"), indent=2)