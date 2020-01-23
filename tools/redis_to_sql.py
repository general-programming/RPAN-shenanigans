import os
import datetime
import json

from redis import StrictRedis
from rpandumper.model import sm, SeedResponse, Comment

db = sm()
redis = StrictRedis(
    os.environ.get("REDIS_URL", "localhost"),
    decode_responses=True
)

# Write out seed responses
for raw_response, score in redis.zrange("rpan:events:seed_response", 0, -1, withscores=True):
    timestamp = datetime.datetime.utcfromtimestamp(score)
    try:
        response = json.loads(raw_response)
    except json.JSONDecodeError:
        response = {"text": raw_response}

    new_response = SeedResponse(
        time=timestamp,
        response=response
    )
    db.add(new_response)
    db.commit()
    redis.zrem("rpan:events:seed_response", raw_response)

# Write out comment responses
comments_found = 0

for key in redis.keys("rpan:events:socket:comments*"):
    comments_found += 1
    print(key, comments_found, redis.zcard(key))
    for raw_response, score in redis.zrange(key, 0, -1, withscores=True):
        timestamp = datetime.datetime.utcfromtimestamp(score)
        response = json.loads(raw_response)

        new_comment = Comment(
            time=timestamp,
            response=response
        )
        db.add(new_comment)
    db.commit()

    for raw_response in redis.zrange(key, 0, -1):
        redis.zrem(key, raw_response)
