from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext

from .config import settings

security = HTTPBasic()

USERNAME = settings.DEFAULT_USERNAME
PASSWORD = settings.DEFAULT_PASSWORD

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
hashed_password = pwd_context.hash(PASSWORD)


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    if credentials.username != USERNAME or not verify_password(credentials.password, hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
