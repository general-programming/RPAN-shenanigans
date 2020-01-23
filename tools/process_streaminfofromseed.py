import os
import json

from redis import StrictRedis
from rpandumper.model import sm, SeedResponse, Stream

db = sm()
db_query = sm()
done_streams = set()

for seed in db_query.query(SeedResponse).yield_per(50):
    if "text" in seed.response or seed.response["status"] == "User ID is not found":
        continue

    if "data" not in seed.response:
        print(seed.response)

    for data in seed.response["data"]:
        if data["post"]["id"] in done_streams:
            continue

        stream = db.query(Stream).filter(Stream.post_id == data["post"]["id"]).scalar()

        if not stream:
            if data["post"]["authorInfo"]["__typename"] == "UnavailableRedditor":
                authorname = "unknownUser-UnavailableRedditor"
            else:
                authorname = data["post"]["authorInfo"]["name"]

            stream = Stream(
                post_id=data["post"]["id"],
                author=authorname,
                title=data["post"]["title"],
                hls_id=data["stream"]["stream_id"]
            )
            db.add(stream)
        done_streams.add(stream.post_id)

    db.commit()
    print(seed.id, len(seed.response["data"]))