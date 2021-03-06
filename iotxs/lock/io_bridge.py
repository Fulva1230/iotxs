import asyncio
import re
import signal
from datetime import datetime

import anyio
from anyio import create_task_group
from dependency_injector import containers, providers
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from paho.mqtt.client import Client as MqttClient
from pydantic import ValidationError

from iotxs.config import DATABASE_NAME, LOCK_REQ_RECORD_COLLECTION_NAME, LOCK_NOTIFICATION_RECORD_COLLECTION_NAME
from iotxs.io.io import init_mongo_client, init_mqtt_client
from iotxs.lock.record_types import LockReqRecord, LockNotificationRecord
from iotxs.msg_types import LockCommand

SUBSCRIPTION_NAME_PATTERN = "iotxs/+/lock"
SUBSCRIPTION_QoS = 0
CLIENT_NAME_PATTERN = re.compile(".*?/(.*?)/")
PUBLISH_NAME_PATTERN = "iotxs/{client}/lock/notification"
SERVER_HOST = "10.144.69.132"
DB_CONNECTION_STRING = "mongodb://aprilab:bossboss@{server}".format(server=SERVER_HOST)


class MqttConnector:
    mqtt_client: MqttClient
    mongo_client: AsyncIOMotorClient
    lock_req_record_list: list[LockReqRecord]

    def __init__(self, mongo_client: AsyncIOMotorClient,
                 mqtt_client: MqttClient):
        self.mqtt_client = mqtt_client
        self.mongo_client = mongo_client
        self.lock_req_record_list = []

    async def task(self):
        async with create_task_group() as tg:
            tg.start_soon(self._lock_req_record_db_push)
            tg.start_soon(self._mqtt_client_subscribe)
            tg.start_soon(self._lock_notification_publish_task)

    async def _lock_req_record_db_push(self):
        while True:
            if len(self.lock_req_record_list) != 0:
                while len(self.lock_req_record_list) > 0:
                    res = await self.mongo_client[DATABASE_NAME][
                        LOCK_REQ_RECORD_COLLECTION_NAME].insert_many(
                        [req_record.dict() for req_record in self.lock_req_record_list])
                    del self.lock_req_record_list[:len(res.inserted_ids)]
            await asyncio.sleep(0.01)

    def _lock_msg_callback(self, client, userdata, msg):
        try:
            logger.debug("got msg!")
            self.lock_req_record_list.append(
                LockReqRecord(client=re.search(CLIENT_NAME_PATTERN, msg.topic).group(1),
                              command=LockCommand.parse_raw(msg.payload), datetime=datetime.now())
            )
        except ValidationError as e:
            ...

    async def _mqtt_client_subscribe(self):
        logger.debug("setup msg callback")
        self.mqtt_client.subscribe(SUBSCRIPTION_NAME_PATTERN, SUBSCRIPTION_QoS)
        self.mqtt_client.message_callback_add(SUBSCRIPTION_NAME_PATTERN, self._lock_msg_callback)

    async def _lock_notification_publish(self, lock_notification_record: LockNotificationRecord):
        self.mqtt_client.publish(PUBLISH_NAME_PATTERN.format(client=lock_notification_record.client),
                                 lock_notification_record.lock_notification.json(exclude_none=True))

    async def _lock_notification_publish_task(self):
        async with self.mongo_client[DATABASE_NAME][
            LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].watch() as change_stream:
            while True:
                next = await change_stream.try_next()
                if next is not None:
                    try:
                        await self._lock_notification_publish(LockNotificationRecord.parse_obj(next['fullDocument']))
                    except ValidationError as e:
                        ...


async def init_terminate_callback(cancel_scope: anyio.CancelScope):
    def handler(signum, frame):
        logger.info("Received Ctrl+C")
        cancel_scope.cancel()

    signal.signal(signal.SIGINT, handler)


class Container(containers.DeclarativeContainer):
    mongo_client = providers.Resource(init_mongo_client)
    mqtt_client = providers.Resource(init_mqtt_client)
    mqtt_connector = providers.Singleton(MqttConnector, mongo_client=mongo_client, mqtt_client=mqtt_client)


async def main_impl():
    container = Container()
    await container.init_resources()
    try:
        logger.info("started!")
        mqtt_connector = await container.mqtt_connector()
        async with create_task_group() as tg:
            tg.start_soon(mqtt_connector.task)
            await init_terminate_callback(tg.cancel_scope)
    finally:
        await container.shutdown_resources()
        logger.info("cleaned up")


def main():
    asyncio.run(main_impl())


if __name__ == "__main__":
    main()
