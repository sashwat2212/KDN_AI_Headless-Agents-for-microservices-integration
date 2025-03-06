import redis
import hashlib
import json
import os

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
MESSAGE_TTL = int(os.getenv("MESSAGE_TTL", 900))  

class RedisDeduplication:
    def __init__(self):
        self.redis_client = redis.StrictRedis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
        )

    def _generate_message_id(self, message: dict) -> str:
        message_str = json.dumps(message, sort_keys=True)
        return hashlib.sha256(message_str.encode()).hexdigest()

    def is_duplicate(self, message: dict) -> bool:
        message_id = self._generate_message_id(message)
        
        if self.redis_client.exists(message_id):
            print(f"[Deduplication] Duplicate message detected: {message_id}")
            return True
        
        self.redis_client.setex(message_id, MESSAGE_TTL, "1")
        return False
