import os
import json
import time
import random
import datetime as dt
import boto3
from botocore.config import Config
from faker import Faker

# Firehose config
delivery_stream_name = "firehose-events-users-visit-pages"
region = "us-east-1"

# AWS client
fh = boto3.client("firehose", region_name=region, config=Config(retries={"max_attempts": 10}))

# Faker instance
fake = Faker()

# Example set of page URLs
pages = [
    "/home",
    "/products",
    "/products/item1",
    "/products/item2",
    "/cart",
    "/checkout",
    "/search?q=shoes",
    "/search?q=jackets",
    "/about",
    "/contact"
]

def mk_event(i):
    now = dt.datetime.utcnow()
    return {
        "event_id": i,
        "user_id": f"user_{random.randint(1, 1000)}",
        "page_url": random.choice(pages),
        "user_agent": fake.user_agent(),
        "ip_address": fake.ipv4_public(),
        "referrer": fake.uri(),
        "event_ts": now.isoformat(),
        "year": f"{now.year:04d}",
        "month": f"{now.month:02d}",
        "day": f"{now.day:02d}"
    }

def put_batch(batch_size=100, total=10000):
    buf = []
    for i in range(total):
        ev = mk_event(i)
        print(ev)
        buf.append({"Data": (json.dumps(ev) + "\n").encode("utf-8")})
        if len(buf) >= batch_size:
            fh.put_record_batch(DeliveryStreamName=delivery_stream_name, Records=buf)
            buf.clear()
    if buf:
        fh.put_record_batch(DeliveryStreamName=delivery_stream_name, Records=buf)

if __name__ == "__main__":
    put_batch()
    print("done")
