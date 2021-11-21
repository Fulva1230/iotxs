import asyncio
import re

from . import connectivity
from rx.core.typing import Observable as ObservableType
from pydantic import BaseModel, ValidationError
from .msg_types import LockCommand
from logging import Logger
from typing import Optional

# Subscribe to the topic "iotxs/+/lock" where + is the name of the client
# Publish to the topic "iotxs/+/lock_notification
#

SUBSCRIPTION_NAME_PATTERN = "iotxs/+/lock"
CLIENT_NAME_PATTERN = re.compile(".*?/(.*?)/")
life_state = False
DATABASE_NAME = "iotxs"
LOCK_REQ_RECORD_COLLECTION_NAME = "lock_req_record"

logger: Optional[Logger] = None


class LockReqRecord(BaseModel):
    client: str
    command: LockCommand


lock_req_record_list: list[LockReqRecord] = []


async def _lock_req_record_db_push():
    while life_state:
        if len(lock_req_record_list) != 0:
            try:
                await connectivity.mongo_client[DATABASE_NAME][LOCK_REQ_RECORD_COLLECTION_NAME].insert_many(
                    (req_record.dict() for req_record in lock_req_record_list))
                lock_req_record_list.clear()
            except BaseException as e:
                if logger is not None:
                    logger.exception(e)
        await asyncio.sleep(0)


def _lock_msg_callback(client, userdata, msg):
    try:
        lock_req_record_list.append(
            LockReqRecord(client=re.search(CLIENT_NAME_PATTERN, msg.topic).group(1),
                          command=LockCommand.parse_raw(msg.payload))
        )
        logger.debug("got msg")
    except ValidationError as e:
        print(e)


async def _mqtt_client_subscribe():
    connectivity.mqtt_client.subscribe(SUBSCRIPTION_NAME_PATTERN, 2)
    connectivity.mqtt_client.message_callback_add(SUBSCRIPTION_NAME_PATTERN, _lock_msg_callback)


def init():
    global life_state
    life_state = True
    connectivity.coroutine_reqs.append(_lock_req_record_db_push())
    connectivity.coroutine_reqs.append(_mqtt_client_subscribe())


def deinit():
    global life_state
    life_state = False
    connectivity.mqtt_client.unsubscribe(SUBSCRIPTION_NAME_PATTERN, 2)
    connectivity.mqtt_client.message_callback_remove(SUBSCRIPTION_NAME_PATTERN)


def owner_change() -> ObservableType[str]:
    ...
