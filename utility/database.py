from motor.motor_asyncio import AsyncIOMotorClient

from .config import settings

client = AsyncIOMotorClient(settings.DATABASE_URL)
database = client[settings.DATABASE_NAME]
cpe_collection = database.get_collection("cpe")


async def ensure_collection_exists():
    await cpe_collection.insert_one({"_id": "dummy_id"})
    await cpe_collection.delete_one({"_id": "dummy_id"})


async def create_indexes():
    await ensure_collection_exists()

    await cpe_collection.create_index("cpe_name", unique=True)
