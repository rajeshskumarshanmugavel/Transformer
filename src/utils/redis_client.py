import redis
from src.utils.config import CONFIG

r = redis.StrictRedis(
    host=CONFIG['REDIS_HOST'],
    port=CONFIG['REDIS_PORT'],
    password=CONFIG['REDIS_PASSWORD'],
    ssl=CONFIG['EXEC_ENVIRONMENT'] != 'local'
)

class RedisClient:
    def __init__(self):
        self.client = r

    def get(self, key):
        return self.client.get(key).decode('utf-8') if self.client.exists(key) else None

    def set(self, key, value, ex=None):
        return self.client.set(key, value, ex=ex)

    def delete(self, key):
        return self.client.delete(key)
