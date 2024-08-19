import os

import fastapi_offline_swagger_ui
from fastapi import FastAPI, applications
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.staticfiles import StaticFiles

from utility import endpoints

app = FastAPI(openapi_url="/cpe/api/openapi.json",
              docs_url="/cpe/api/docs",
              )

app.mount("/static", StaticFiles(directory="static"), name="static")

assets_path = fastapi_offline_swagger_ui.__path__[0]

if os.path.exists(assets_path + "/swagger-ui.css") and os.path.exists(assets_path + "/swagger-ui-bundle.js"):
    app.mount("/cpe/api/assets", StaticFiles(directory=assets_path), name="static")


    def swagger_monkey_patch(*args, **kwargs):
        return get_swagger_ui_html(
            *args,
            **kwargs,
            swagger_favicon_url="",
            swagger_css_url="/cpe/api/assets/swagger-ui.css",
            swagger_js_url="/cpe/api/assets/swagger-ui-bundle.js",
        )


    applications.get_swagger_ui_html = swagger_monkey_patch

app.include_router(endpoints.router, prefix='/cpe/api')
