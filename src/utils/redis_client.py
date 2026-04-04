class MockRedisClient:
    async def xadd(self, key: str, data: dict):
        pass


def get_redis_client():
    return MockRedisClient()
