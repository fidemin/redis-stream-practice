import logging
import random

import redis

redis_client = redis.Redis(
    host='localhost',
    port=6379,
    decode_responses=True
)

logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    # Example usage
    action = "create"
    resource = "document"
    metadata = {"details": "Sample metadata"}

    # Assuming the write_audit_log function is defined in the same file
    from main.core.producer import write_audit_log

    num_of_messages = random.randint(10, 20)
    for i in range(num_of_messages):
        user_id = f"user_{random.randint(1, 100)}"
        write_audit_log(redis_client, user_id, action, resource, metadata)

    logging.info(f"Produced {num_of_messages} messages to the stream.")
