import os
import json

from rpandumper.model import sm, SeedResponse, Stream
from rpandumper.common import create_redis

# Connections
redis = create_redis()
db = sm()
db_query = sm()

# Completed lists, bad on ram eventually
done_streams = set(x[0] for x in db.query(Stream.post_id))
done_seeds = [int(x) for x in redis.smembers("rpan:processedseeds")]
total_streams = db.query(SeedResponse).filter(SeedResponse.id.notin_(done_seeds)).count()

for seed in db_query.query(SeedResponse).filter(SeedResponse.id.notin_(done_seeds)).yield_per(50):
    if "text" in seed.response or seed.response["status"] == "User ID is not found":
        redis.sadd("rpan:processedseeds", seed.id)
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
    print(f"{seed.id}/{total_streams}", len(seed.response["data"]))
    redis.sadd("rpan:processedseeds", seed.id)
