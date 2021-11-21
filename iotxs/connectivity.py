"""
The module brings all the connectivity
"""

from typing import Optional, Union, Literal, Coroutine
from threading import Thread
import asyncio
import paho.mqtt.client as mqtt
from motor.motor_asyncio import AsyncIOMotorClient
from logging import Logger

SERVER_HOST = "10.144.69.132"
DB_CONNECTION_STRING = "mongodb://aprilab:bossboss@{server}".format(server=SERVER_HOST)

logger: Optional[Logger] = None


def own_thread_func():
    async def async_main():
        global life_state
        global mongo_client
        global unfinished_task
        mongo_client = AsyncIOMotorClient(DB_CONNECTION_STRING)
        mqtt_client.connect(SERVER_HOST, 1883, 60)
        while life_state in ["ENABLED", "STOP_REQ"]:
            await push_requsted_tasks()
            await loop_mqtt_client()
            await asyncio.sleep(0)
        clean_up()

    asyncio.run(async_main())


mqtt_client = mqtt.Client()
mongo_client: Optional[AsyncIOMotorClient]

thread: Optional[Thread] = Thread(target=own_thread_func)
unfinished_task: list[asyncio.Task] = []
life_state: Literal["ENABLED", "DISABLED", "STOP_REQ"] = "DISABLED"
coroutine_reqs: list[Coroutine] = []


async def loop_mqtt_client():
    mqtt_client.loop_read()
    mqtt_client.loop_write()
    mqtt_client.loop_misc()


async def push_requsted_tasks():
    global unfinished_task
    unfinished_task = [task for task in unfinished_task if task.done() is not True]
    unfinished_task.extend([asyncio.create_task(coroutine) for coroutine in coroutine_reqs])
    coroutine_reqs.clear()


def clean_up():
    if mongo_client is not None:
        mongo_client.close()
    mqtt_client.disconnect()
    logger.info("Successfully connectivity resources released") if logger is not None else ...


def init():
    global life_state
    life_state = "ENABLED"
    thread.start()


def deinit():
    async def impl():
        global life_state
        life_state = "STOP_REQ"
        while len(unfinished_task) != 0:
            await asyncio.sleep(0)
        life_state = "DISABLED"

    async def task_creator():
        asyncio.create_task(impl())

    coroutine_reqs.append(task_creator())
