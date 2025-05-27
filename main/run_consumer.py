import logging

import redis

from main.core.consumer import run_one_consumer

logging.basicConfig(level=logging.INFO)

redis_client = redis.Redis(
    host='localhost',
    port=6379,
    decode_responses=True
)


if __name__ == "__main__":
    # Start the consumer
    run_one_consumer(redis_client)
