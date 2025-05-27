import logging
import threading
import signal

import redis

from main.core.consumer import run_one_consumer

logging.basicConfig(level=logging.INFO)

redis_client = redis.Redis(
    host='localhost',
    port=6379,
    decode_responses=True
)

# this is easiest way to handle signals
stop_event = threading.Event()

def handle_shutdown(signum, frame):
    logging.info(f"Received shutdown signal ({signum}), stopping consumer...")
    stop_event.set()

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_shutdown)  # Ctrl+C or SIGINT
    signal.signal(signal.SIGTERM, handle_shutdown) # pod shutdown or SIGTERM

    run_one_consumer(redis_client, stop_event)
