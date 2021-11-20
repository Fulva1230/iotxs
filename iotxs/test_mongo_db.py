import asyncio
import json
import logging
import functools

from . import try_fastapi
import paho.mqtt.client as mqtt
from datetime import datetime
from .datatypes import DatetimeContent


def test_mongo_insert():
    async def main():
        try_fastapi.mongo_client = try_fastapi.AsyncIOMotorClient(try_fastapi.DB_CONNECTION_STRING)
        await try_fastapi.push_hello_world()

    asyncio.run(main())


def test_mqtt_save():
    mqtt_client = mqtt.Client()

    async def ivloop():
        tasks = []

        def on_connect(client, userdata, flags, rc):
            async def emit_test_msg():
                i = 0
                while i < 10:
                    if mqtt_client.is_connected():
                        msg = DatetimeContent(content="hello {}".format(i), datetime=datetime.now())
                        mqtt_client.publish("test_save", msg.json())
                        i += 1
                    await asyncio.sleep(0.1)

            tasks.append(asyncio.create_task(emit_test_msg()))

        async def loop_before_connected():
            while not mqtt_client.is_connected():
                mqtt_client.loop_read()
                mqtt_client.loop_write()
                await asyncio.sleep(0)

        mqtt_client.on_connect = on_connect
        mqtt_client.connect("10.144.69.132", 1883, 60)
        await loop_before_connected()
        while not functools.reduce(lambda x, y: x and y, (task.done() for task in tasks), True):
            mqtt_client.loop_read()
            mqtt_client.loop_write()
            await asyncio.sleep(0)
        mqtt_client.disconnect()

    asyncio.run(ivloop())
