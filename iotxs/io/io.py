from collections import Generator

from motor.motor_asyncio import AsyncIOMotorClient
from paho.mqtt.client import Client as MqttClient
import asyncio

SERVER_HOST = "10.144.69.132"
DB_CONNECTION_STRING = "mongodb://aprilab:bossboss@{server}".format(server=SERVER_HOST)


async def init_mongo_client() -> Generator[AsyncIOMotorClient, None, None]:
    mongo_client = AsyncIOMotorClient(DB_CONNECTION_STRING)
    yield mongo_client
    mongo_client.close()


async def init_mqtt_client() -> Generator[MqttClient, None, None]:
    mqtt_client = MqttClient()
    mqtt_client.connect(SERVER_HOST, 1883, 60)

    async def read_write():
        while True:
            mqtt_client.loop_read()
            mqtt_client.loop_write()
            await asyncio.sleep(0.02)

    async def misc_loop():
        while True:
            mqtt_client.loop_misc()
            await asyncio.sleep(3.0)

    read_write_task = asyncio.create_task(read_write())
    misc_loop_task = asyncio.create_task(misc_loop())
    yield mqtt_client
    read_write_task.cancel()
    misc_loop_task.cancel()
    mqtt_client.disconnect()
