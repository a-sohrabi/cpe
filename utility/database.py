from motor.motor_asyncio import AsyncIOMotorClient

from .config import settings

client = AsyncIOMotorClient(settings.DATABASE_URL)
database = client[settings.DATABASE_NAME]
vulnerability_collection = database.get_collection("nvd")
