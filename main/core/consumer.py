import logging

import redis
import time

from main.core.constants import STREAM_NAME

# Config
GROUP_NAME = "audit_consumer_group"
CONSUMER_NAME = "worker-1"
BLOCK_TIME_MS = 5000
MAX_MESSAGES_PER_READ = 10


logger = logging.getLogger(__name__)

def ensure_consumer_group(redis_client):
    try:
        redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        logger.info(f"Consumer group '{GROUP_NAME}' created.")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP Consumer Group name already exists" in str(e):
            logger.info(e)
            pass
        else:
            raise

def process_messages(messages):
    count = 0
    for message_id, data in messages:
        logger.info(f"[{CONSUMER_NAME}] Processing {message_id}: {data}")
        count += 1

    logger.info(f"[{CONSUMER_NAME}] Processed {count} messages.")
    time.sleep(0.05 * count)  # Simulate processing time



def run_one_consumer(redis_client):
    logger.info(f"Starting consumer: {CONSUMER_NAME}")
    ensure_consumer_group(redis_client)
    while True:
        try:
            messages = redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=MAX_MESSAGES_PER_READ,
                block=BLOCK_TIME_MS
            )

            if not messages:
                logger.info(f"[{CONSUMER_NAME}] No new messages, waiting...")
                continue

            for stream, entries in messages:
                process_messages(entries)
                message_ids = [message_id for message_id, data in entries]
                redis_client.xack(STREAM_NAME, GROUP_NAME, *message_ids)

        except redis.exceptions.RedisError as err:
            logger.error(f"Redis error: {err}")
            time.sleep(5)
