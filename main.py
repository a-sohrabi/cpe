import os

import fastapi_offline_swagger_ui
from fastapi import Depends, FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles

from utility import endpoints
from utility.auth import authenticate
from utility.database import create_indexes

app = FastAPI(openapi_url="/cpe/api/openapi.json",
              docs_url=None)  # Disable the default docs endpoint

app.mount("/static", StaticFiles(directory="static"), name="static")

assets_path = fastapi_offline_swagger_ui.__path__[0]

if os.path.exists(assets_path + "/swagger-ui.css") and os.path.exists(assets_path + "/swagger-ui-bundle.js"):
    app.mount("/cpe/api/assets", StaticFiles(directory=assets_path), name="static")


@app.on_event("startup")
async def startup_event():
    await create_indexes()


# Override the swagger UI HTML with basic auth protection
@app.get("/cpe/api/docs", include_in_schema=False)
async def get_swagger_ui(credentials: HTTPBasicCredentials = Depends(authenticate)):
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title="Swagger UI",
        swagger_favicon_url="",
        swagger_css_url="/cpe/api/assets/swagger-ui.css",
        swagger_js_url="/cpe/api/assets/swagger-ui-bundle.js",
    )


app.include_router(endpoints.router, prefix='/cpe/api', tags=['CPE'])
