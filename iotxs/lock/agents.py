from .coordinator import StateAgent, EventAgent, Coordinator
from .record_types import LockNotificationRecord, LockStateRecord, LockReqRecord
from typing import Optional, Union, Literal, Coroutine, Callable
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from .record_types import DATABASE_NAME, LOCK_STATE_RECORD_COLLECTION_NAME, LOCK_NOTIFICATION_RECORD_COLLECTION_NAME, \
    LOCK_REQ_RECORD_COLLECTION_NAME
import pymongo
from pydantic import ValidationError
from collections import deque
from datetime import datetime


class StateAgentImpl:
    def __init__(self, _mongo_client: AsyncIOMotorClient):
        self._mongo_client = _mongo_client

    async def push_lock_state(self, lock_state_record: LockStateRecord):
        await self._mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME].insert_one(
            lock_state_record.dict()
        )

    async def get_current_state(self) -> LockStateRecord:
        try:
            res = await self._mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME] \
                .find_one(sort=[("datetime", pymongo.DESCENDING)])
            return LockStateRecord.parse_obj(res) if res is not None else LockStateRecord(owner_list=[],
                                                                                          datetime=datetime.now(),
                                                                                          expire_time=datetime.now())
        except ValidationError:
            return LockStateRecord(owner_list=[],
                                   datetime=datetime.now(),
                                   expire_time=datetime.now())

    async def push_lock_notification(self, lock_notification_record: LockNotificationRecord):
        await self._mongo_client[DATABASE_NAME][LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].insert_one(
            lock_notification_record.dict()
        )


class EventAgentImpl(EventAgent):
    _mongo_client: AsyncIOMotorClient
    _pending_processed: deque[LockReqRecord]

    def __init__(self, _mongo_client: AsyncIOMotorClient):
        self._mongo_client = _mongo_client
        self._pending_processed = deque()

    async def listen_lock_req_task(self):
        try:
            async with self._mongo_client[DATABASE_NAME][
                LOCK_REQ_RECORD_COLLECTION_NAME].watch() as change_stream:
                while True:
                    next = await change_stream.try_next()
                    if next is not None:
                        self._pending_processed.append(LockReqRecord.parse_obj(next['fullDocument']))

        except ValidationError as e:
            ...

    async def next_lock_req(self) -> LockReqRecord:
        while True:
            try:
                return self._pending_processed.popleft()
            except IndexError:
                await asyncio.sleep(0.01)
