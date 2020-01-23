import redis
import json

r = redis.StrictRedis()
p = r.pubsub(ignore_subscribe_messages=True)

def handle_rpan_event(event):
    try:
        data = json.loads(event["data"])
    except json.JSONDecodeError:
        print(event["data"])

    try:
        payload = data['payload']
    except KeyError:
        return
    # print(data)
    try:
        print(f"[{payload['link_id']}] {payload['author']}: {payload['rtjson']['document'][0]['c'][0]['t']}")
    except KeyError:
        print(payload)

p.psubscribe(**{"rpan:events:*": handle_rpan_event})
thread = p.run_in_thread(sleep_time=0.001)

try:
    thread.join()
except KeyboardInterrupt:
    thread.stop()
