import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    DATABASE_URL: str = os.getenv('DATABASE_URL')
    DATABASE_NAME: str = os.getenv('DATABASE_NAME')
    CPE_URL: str = os.getenv('CPE_V23_URL')
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv('KAFKA_BOOTSTRAP_SERVER')
    KAFKA_TOPIC: str = os.getenv('KAFKA_TOPIC')
    FILES_BASE_DIR: str = 'data'
    LOKI_URL: str = os.getenv('LOKI_URL')

    class Config:
        env_file = "../.env"


settings = Settings()
