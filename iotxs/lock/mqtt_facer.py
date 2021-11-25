import asyncio
import re

from iotxs import connectivity
from rx.core.typing import Observable as ObservableType
from pydantic import ValidationError

from iotxs.lock.record_types import LockReqRecord, DATABASE_NAME, LOCK_REQ_RECORD_COLLECTION_NAME, \
    LOCK_NOTIFICATION_RECORD_COLLECTION_NAME, LockNotificationRecord
from iotxs.msg_types import LockCommand, LockNotification
from logging import Logger
from typing import Optional
from datetime import datetime

# Subscribe to the topic "iotxs/+/lock" where + is the name of the client
# Publish to the topic "iotxs/+/lock_notification
#

SUBSCRIPTION_NAME_PATTERN = "iotxs/+/lock"
SUBSCRIPTION_QoS = 0
CLIENT_NAME_PATTERN = re.compile(".*?/(.*?)/")
life_state = False
PUBLISH_NAME_PATTERN = "iotxs/{client}/lock_notification"

logger: Optional[Logger] = None

lock_req_record_list: list[LockReqRecord] = []


async def _lock_req_record_db_push():
    global lock_req_record_list
    while life_state:
        if len(lock_req_record_list) != 0:
            try:
                while len(lock_req_record_list) > 0:
                    res = await connectivity.mongo_client[DATABASE_NAME][LOCK_REQ_RECORD_COLLECTION_NAME].insert_many(
                        [req_record.dict() for req_record in lock_req_record_list])
                    del lock_req_record_list[:len(res.inserted_ids)]
            except BaseException as e:
                logger.exception(e) if logger is not None else ...
        await asyncio.sleep(0)


def _lock_msg_callback(client, userdata, msg):
    try:
        lock_req_record_list.append(
            LockReqRecord(client=re.search(CLIENT_NAME_PATTERN, msg.topic).group(1),
                          command=LockCommand.parse_raw(msg.payload), datetime=datetime.now())
        )
        logger.debug("got msg") if logger is not None else ...
    except ValidationError as e:
        logger.exception(e) if logger is not None else ...


async def _mqtt_client_subscribe():
    connectivity.mqtt_client.subscribe(SUBSCRIPTION_NAME_PATTERN, SUBSCRIPTION_QoS)
    connectivity.mqtt_client.message_callback_add(SUBSCRIPTION_NAME_PATTERN, _lock_msg_callback)


async def _lock_notification_publish(lock_notification_record: LockNotificationRecord):
    connectivity.mqtt_client.publish(PUBLISH_NAME_PATTERN.format(client=lock_notification_record.client),
                                     lock_notification_record.lock_notification.json(exclude_none=True))


async def _lock_notification_publish_task():
    try:
        async with connectivity.mongo_client[DATABASE_NAME][
            LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].watch() as change_stream:
            while life_state:
                next = await change_stream.try_next()
                if next is not None:
                    await _lock_notification_publish(LockNotificationRecord.parse_obj(next['fullDocument']))

    except ValidationError as e:
        logger.exception(e) if logger is not None else ...


def init():
    global life_state
    life_state = True
    connectivity.coroutine_reqs.append(_lock_req_record_db_push())
    connectivity.coroutine_reqs.append(_mqtt_client_subscribe())
    connectivity.coroutine_reqs.append(_lock_notification_publish_task())


def deinit():
    global life_state
    life_state = False
    connectivity.mqtt_client.unsubscribe(SUBSCRIPTION_NAME_PATTERN, 2)
    connectivity.mqtt_client.message_callback_remove(SUBSCRIPTION_NAME_PATTERN)
