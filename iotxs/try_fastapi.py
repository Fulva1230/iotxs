from typing import Optional, Union

from fastapi import FastAPI
import asyncio
import paho.mqtt.client as mqtt
from threading import Thread
import logging
from json import JSONEncoder, JSONDecoder
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from .datatypes import DatetimeContent

app = FastAPI()
app_daemons: list[asyncio.Task] = []


@app.get("/")
def read_root():
    return {"Hello": "World"}


SERVER_HOST = "10.144.69.132"
DB_CONNECTION_STRING = "mongodb://aprilab:bossboss@{server}".format(server=SERVER_HOST)

mqtt_client = mqtt.Client()
mqtt_thread: Optional[Thread] = None
mongo_client: Optional[AsyncIOMotorClient]


async def push_hello_world():
    database = mongo_client['iotxs']
    collection = database['hello_world']
    async with await mongo_client.start_session() as s:
        await collection.insert_one({"value": "hello world", "datetime": datetime.now()}, session=s)



async def periodic_publish():
    json_encoder = JSONEncoder()
    while True:
        if mqtt_client.is_connected():
            mqtt_client.publish("hello", json_encoder.encode({"value": "hello world"}))
        await asyncio.sleep(0.5)


def save_to_database(client, userdata, message):
    async def impl():
        database = mongo_client['iotxs']
        collection = database['test_save']
        logger = logging.getLogger("uvicorn.mqtt")
        logger.debug("save to database")
        msg_obj = DatetimeContent.parse_raw(message.payload)
        async with await mongo_client.start_session() as s:
            await collection.insert_one(msg_obj.dict(), session=s)
    asyncio.create_task(impl())


def init_evloop():
    global mqtt_thread
    logger = logging.getLogger("uvicorn.mqtt")

    def impl():
        mqtt_client.connect(SERVER_HOST, 1883, 60)

        async def asyncio_main():
            logger.info("start asyncio event loop")
            app_daemons.append(asyncio.create_task(periodic_publish()))
            global mongo_client
            mongo_client = AsyncIOMotorClient(DB_CONNECTION_STRING)
            mqtt_client.subscribe("test_save")
            mqtt_client.message_callback_add("test_save", save_to_database)
            while len(app_daemons):
                mqtt_client.loop_read()
                mqtt_client.loop_write()
                await asyncio.sleep(0)
            mqtt_client.disconnect()
            logger.info("disconnect to the mqtt broker")

        asyncio.run(asyncio_main())

    mqtt_thread = Thread(target=impl)
    mqtt_thread.start()


@app.on_event("startup")
async def startup_event():
    init_evloop()


@app.on_event("shutdown")
async def startup_event():
    [task.cancel() for task in app_daemons]
    app_daemons.clear()
    mqtt_thread.join()
