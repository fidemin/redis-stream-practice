import logging

import json
from datetime import datetime

from main.core.constants import STREAM_NAME

MAX_STREAM_LENGTH = 100000

logger = logging.getLogger(__name__)

def write_audit_log(redis_client, user_id, action, resource, metadata=None):
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": user_id,
        "action": action,
        "resource": resource,
        "metadata": json.dumps(metadata or {})
    }

    message_id = redis_client.xadd(
        name=STREAM_NAME,
        fields=log_entry,
        maxlen=MAX_STREAM_LENGTH,
        approximate=True
    )
    logger.info(f"Written to stream: ID = {message_id}")
