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
from dependency_injector import containers, providers
from datetime import datetime
from loguru import logger
from anyio import create_task_group

SERVER_HOST = "10.144.69.132"
DB_CONNECTION_STRING = "mongodb://aprilab:bossboss@{server}".format(server=SERVER_HOST)


class StateAgentImpl:
    def __init__(self, _mongo_client: AsyncIOMotorClient):
        self._mongo_client = _mongo_client

    async def push_lock_state(self, lock_state_record: LockStateRecord):
        await self._mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME].insert_one(
            lock_state_record.dict()
        )

    async def get_current_state(self) -> Optional[LockStateRecord]:
        res = await self._mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME] \
            .find_one(sort=[("datetime", pymongo.DESCENDING)])
        return LockStateRecord.parse_obj(res) if res is not None else LockStateRecord(owner_list=[],
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
                await asyncio.sleep(0)


async def init_mongo_client() -> AsyncIOMotorClient:
    mongo_client = AsyncIOMotorClient(DB_CONNECTION_STRING)
    yield mongo_client
    mongo_client.close()


class Container(containers.DeclarativeContainer):
    mongo_client = providers.Resource(init_mongo_client)
    event_agent = providers.Factory(EventAgentImpl, mongo_client)
    state_agent = providers.Factory(StateAgentImpl, mongo_client)
    lock_coordinator = providers.Factory(Coordinator, state_agent=state_agent, event_agent=event_agent)


def main():
    async def impl():
        container = Container()
        await container.init_resources()
        coordinator = await container.lock_coordinator()
        try:
            logger.info("started")
            async with create_task_group() as tg:
                tg.start_soon(coordinator.task)
        finally:
            logger.info("cleaning up")
            await container.shutdown_resources()

    try:
        asyncio.run(impl())
    except KeyboardInterrupt as e:
        logger.info("cleaned up")


if __name__ == "__main__":
    main()
