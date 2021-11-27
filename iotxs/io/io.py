from motor.motor_asyncio import AsyncIOMotorClient
from paho.mqtt.client import Client as MqttClient
import asyncio

SERVER_HOST = "10.144.69.132"
DB_CONNECTION_STRING = "mongodb://aprilab:bossboss@{server}".format(server=SERVER_HOST)


async def init_mongo_client() -> AsyncIOMotorClient:
    mongo_client = AsyncIOMotorClient(DB_CONNECTION_STRING)
    yield mongo_client
    mongo_client.close()


async def init_mqtt_client() -> MqttClient:
    mqtt_client = MqttClient()
    mqtt_client.connect(SERVER_HOST, 1883, 60)

    async def refresh():
        while True:
            mqtt_client.loop_read()
            mqtt_client.loop_write()
            mqtt_client.loop_misc()
            await asyncio.sleep(0)

    refresh_task = asyncio.create_task(refresh())
    yield mqtt_client
    refresh_task.cancel()
    mqtt_client.disconnect()
