from prometheus_client import start_http_server, Gauge
import random
import time

from redis import StrictRedis

r = StrictRedis(decode_responses=True)
gauges = {}

def update_gauge(fetchkey: str):
    data = r.hgetall("rpan:science:" + fetchkey)
    for key, value in data.items():
        gaugekey = f"{fetchkey}_{key}"

        try:
            value = int(value)
        except (TypeError, ValueError) as e:
            print(f"{value} is not an integer?")
        
        if gaugekey not in gauges:
            gauges[gaugekey] = Gauge(gaugekey, '')

        gauges[gaugekey].set(value)

def update_gauges():
    update_gauge("socketdumper")
    update_gauge("hlsdumper")

if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)

    # Load the gauges every 3 seconds.
    try:
        while True:
            update_gauges()
            time.sleep(3)
    except KeyboardInterrupt:
        pass