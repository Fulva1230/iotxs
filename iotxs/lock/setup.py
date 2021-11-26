from .coordinator import StateAgent, EventAgent, Coordinator
from .record_types import LockNotificationRecord, LockStateRecord, LockReqRecord
from typing import Optional, Union, Literal, Coroutine, Callable
from threading import Thread, current_thread
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from .record_types import DATABASE_NAME, LOCK_STATE_RECORD_COLLECTION_NAME, LOCK_NOTIFICATION_RECORD_COLLECTION_NAME, \
    LOCK_REQ_RECORD_COLLECTION_NAME
import pymongo
from pydantic import ValidationError

mongo_client: AsyncIOMotorClient
thread: Thread
_enabled: False

SERVER_HOST = "10.144.69.132"
DB_CONNECTION_STRING = "mongodb://aprilab:bossboss@{server}".format(server=SERVER_HOST)


class StateAgentImpl:
    async def push_lock_state(self, lock_state_record: LockStateRecord):
        await mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME].insert_one(
            lock_state_record.dict()
        )

    async def get_current_state(self) -> Optional[LockStateRecord]:
        res = await mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME] \
            .find_one(sort=[("datetime", pymongo.DESCENDING)])
        return LockStateRecord.parse_obj(res) if res is not None else None

    async def push_lock_notification(self, lock_notification_record: LockNotificationRecord):
        await mongo_client[DATABASE_NAME][LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].insert_one(
            lock_notification_record.dict()
        )


class EventAgentImpl:
    listeners: list[Callable[[LockReqRecord], None]]
    event_task: asyncio.Task

    def __init__(self):
        self.listeners = []

    def call_listeners(self, lock_req_record: LockReqRecord):
        [listener(lock_req_record) for listener in self.listeners]

    async def first_listener_init(self):
        try:
            async with mongo_client[DATABASE_NAME][
                LOCK_REQ_RECORD_COLLECTION_NAME].watch() as change_stream:
                while True:
                    next = await change_stream.try_next()
                    if next is not None:
                        self.call_listeners(LockReqRecord.parse_obj(next['fullDocument']))
        except ValidationError as e:
            ...

    def listen_on_lock_req(self, callback: Callable[[LockReqRecord], None]):
        if len(self.listeners) == 0:
            self.event_task = asyncio.create_task(self.first_listener_init())
        self.listeners.append(callback)

    def stop_listen_on_lock_req(self):
        self.event_task.cancel()
        self.listeners.clear()


class WiredCoordinator(Coordinator):
    state_agent = StateAgentImpl
    event_agent = EventAgentImpl


def _clean_up():
    mongo_client.close()


def own_thread_func():
    async def async_main():
        global mongo_client
        mongo_client = AsyncIOMotorClient(DB_CONNECTION_STRING)
        while _enabled:
            await asyncio.sleep(0)
        _clean_up()

    asyncio.run(async_main())


def init():
    global _enabled
    _enabled = True
    thread = Thread(target=own_thread_func)
    thread.start()


def deinit():
    global _enabled
    _enabled = False


def wait_for_deinit_finished():
    if thread is not current_thread():
        thread.join()
