import logging

import redis

redis_client = redis.Redis(
    host='localhost',
    port=6379,
    decode_responses=True
)

logging.basicConfig(level=logging.INFO)


if __name__ == "__main__":
    # Example usage
    user_id = "user123"
    action = "create"
    resource = "document"
    metadata = {"details": "Sample metadata"}

    # Assuming the write_audit_log function is defined in the same file
    from main.core.producer import write_audit_log

    write_audit_log(redis_client, user_id, action, resource, metadata)
