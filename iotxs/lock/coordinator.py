import asyncio
from datetime import datetime, timedelta

from iotxs import connectivity
from .record_types import LockReqRecord, DATABASE_NAME, LOCK_STATE_RECORD_COLLECTION_NAME, \
    LOCK_REQ_RECORD_COLLECTION_NAME, LockStateRecord, LockNotificationRecord, LOCK_NOTIFICATION_RECORD_COLLECTION_NAME
from pydantic import ValidationError
from logging import Logger
from typing import Optional, Callable, Awaitable, Coroutine
import pymongo
from iotxs.msg_types import LockNotification

life_state = False
logger: Optional[Logger] = None
deinit_listeners: list[Coroutine] = []


class Transition:
    req_record: Optional[LockReqRecord]
    lock_state: LockStateRecord

    def __init__(self, lock_state: LockStateRecord, req_record: Optional[LockReqRecord]):
        self.lock_state = lock_state
        self.req_record = req_record
        self.current_time = datetime.now()

    def take(self):
        ...

    def next_lock_state(self) -> LockStateRecord:
        ...

    def lock_notifications(self) -> list[LockNotificationRecord]:
        ...


async def get_current_state():
    res = await connectivity.mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME] \
        .find_one(sort=[("datetime", pymongo.DESCENDING)])
    return LockStateRecord.parse_obj(res) if res is not None else None


async def push_lock_state(lock_state: LockStateRecord):
    connectivity.mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME].insert_one(
        lock_state.dict()
    )


async def push_notification_record(record: LockNotificationRecord):
    await connectivity.mongo_client[DATABASE_NAME][LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].insert_one(
        record.dict()
    )


async def expand_expire_time(lock_state: LockStateRecord, expanded_time: datetime):
    new_lock_state = lock_state.copy()
    new_lock_state.expire_time = expanded_time
    return new_lock_state


async def switch_owner(lock_state: LockStateRecord, expire_time: datetime):
    new_lock_state = lock_state.copy()
    new_lock_state.owner = new_lock_state.pending_clients.pop(0)
    new_lock_state.expire_time = expire_time
    return new_lock_state


async def schedule_expire_action():
    async def delay_for_expire():
        await asyncio.sleep(2.01)
        await fresh_lock_state()

    connectivity.coroutine_reqs.append(delay_for_expire())


async def fresh_lock_state():
    current_lock_state = await get_current_state()
    await lock_state_update(current_lock_state)


async def lock_state_update(current_lock_state: LockStateRecord):
    current_time = datetime.now()
    if current_lock_state.expire_time < current_time:
        await push_notification_record(LockNotificationRecord(
            client=current_lock_state.owner,
            lock_notification=LockNotification(state="RELEASED"),
            datetime=current_time
        ))
        if len(current_lock_state.pending_clients) > 0:
            new_lock_state = await switch_owner(current_lock_state, current_time + timedelta(seconds=2.0))
            new_lock_state.datetime = current_time
            await push_lock_state(new_lock_state)
            await push_notification_record(LockNotificationRecord(
                client=new_lock_state.owner,
                lock_notification=LockNotification(state="TOOK", expire_time=new_lock_state.expire_time),
                datetime=current_time
            ))
            await schedule_expire_action()
        else:
            new_lock_state = current_lock_state.copy()
            new_lock_state.datetime = current_time
            new_lock_state.owner = "noone"
            await push_lock_state(new_lock_state)


async def first_lock_state(req_record: LockReqRecord):
    current_time = datetime.now()
    expire_time = current_time + timedelta(seconds=2.0)
    lock_state = LockStateRecord(owner=req_record.client, pending_clients=[], datetime=current_time,
                                 expire_time=expire_time)
    await push_lock_state(lock_state)
    await push_notification_record(
        LockNotificationRecord(
            client=req_record.client,
            lock_notification=LockNotification(state="TOOK", expire_time=expire_time),
            datetime=current_time
        )
    )
    await schedule_expire_action()


async def new_req_callback(req_record: LockReqRecord):
    current_time = datetime.now()
    logger.debug("got the callback")
    logger.debug(req_record)
    current_state = await get_current_state()
    if current_state is not None:
        if current_state.expire_time >= current_time and current_state.owner == req_record.client:
            expanded_time = current_time + timedelta(seconds=2.0)
            new_lock_state = await expand_expire_time(current_state, expanded_time)
            new_lock_state.datetime = current_time
            await push_lock_state(new_lock_state)
            await schedule_expire_action()
            await push_notification_record(LockNotificationRecord(
                client=req_record.client,
                lock_notification=LockNotification(state="HOLD", expire_time=expanded_time),
                datetime=current_time
            ))
        else:
            if req_record.client not in current_state.pending_clients:
                new_lock_state = current_state.copy()
                new_lock_state.datetime = current_time
                new_lock_state.pending_clients.append(req_record.client)
                if current_state.expire_time >= current_time:
                    await push_lock_state(new_lock_state)
                    await push_notification_record(LockNotificationRecord(
                        client=req_record.client,
                        lock_notification=LockNotification(state="PENDING"),
                        datetime=current_time
                    ))
                else:
                    await lock_state_update(new_lock_state)

            else:
                await push_notification_record(LockNotificationRecord(
                    client=req_record.client,
                    lock_notification=LockNotification(state="NOOP"),
                    datetime=current_time
                ))
    else:
        await first_lock_state(req_record)


async def setup_new_req_callback():
    try:
        async with connectivity.mongo_client[DATABASE_NAME][LOCK_REQ_RECORD_COLLECTION_NAME].watch() as change_stream:
            while life_state:
                next = await change_stream.try_next()
                if next is not None:
                    await new_req_callback(LockReqRecord.parse_obj(next['fullDocument']))

    except ValidationError as e:
        logger.exception(e)


def init():
    global life_state
    life_state = True
    connectivity.coroutine_reqs.append(setup_new_req_callback())


async def deinit_deinit_listeners():
    [await deinit_listener for deinit_listener in deinit_listeners]


def deinit():
    global life_state
    life_state = False
    connectivity.coroutine_reqs.append(deinit_deinit_listeners())
