import logging
from typing import Optional, Union

from fastapi import FastAPI
from . import connectivity
from . import msg_types
from datetime import datetime
import asyncio

app = FastAPI()
app_daemons: list[asyncio.Task] = []


@app.get("/")
def publish_content():
    async def publish():
        connectivity.mqtt_client.publish("hello",
                                         msg_types.DatetimeContent(content="Morning", datetime=datetime.now()).json())

    connectivity.coroutine_reqs.append(publish())
    return {"Hello": "World"}


@app.on_event("startup")
async def startup_event():
    connectivity.logger = logging.getLogger("uvicorn.connectivity")
    connectivity.logger.setLevel(logging.DEBUG)
    connectivity.init()


@app.on_event("shutdown")
async def startup_event():
    connectivity.deinit()
    connectivity.thread.join()
