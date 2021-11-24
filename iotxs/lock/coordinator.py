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
    current_time: datetime
    next_lock_state: LockStateRecord
    lock_notifications: list[LockNotificationRecord]
    next_updates: list[datetime]

    def __init__(self, lock_state: LockStateRecord, req_record: Optional[LockReqRecord], moment: datetime):
        self.lock_state = lock_state
        self.req_record = req_record
        self.current_time = moment
        self.lock_notifications = []
        self.next_updates = []

    def _expire_move(self):
        self.next_lock_state.datetime = self.current_time
        if len(self.lock_state.pending_clients) > 0:
            self.next_lock_state.owner = self.next_lock_state.pending_clients.pop(0)
            self.next_lock_state.expire_time = self.current_time + timedelta(seconds=2.0)
            self.lock_notifications.append(LockNotificationRecord(
                client=self.next_lock_state.owner,
                lock_notification=LockNotification(state="TOOK", expire_time=self.next_lock_state.expire_time),
                datetime=self.current_time
            ))
            self.next_updates.append(self.current_time + timedelta(seconds=2.0))
        else:
            self.next_lock_state.owner = "noone"
            self.next_lock_state.expire_time = self.current_time
        self.lock_notifications.append(LockNotificationRecord(
            client=self.lock_state.owner,
            lock_notification=LockNotification(state="RELEASED"),
            datetime=self.current_time
        ))

    def _expand(self):
        self.next_lock_state.datetime = self.current_time
        self.next_lock_state.expire_time = self.current_time + timedelta(seconds=2.0)
        self.lock_notifications.append(LockNotificationRecord(
            client=self.next_lock_state.owner,
            lock_notification=LockNotification(state="HOLD", expire_time=self.next_lock_state.expire_time),
            datetime=self.current_time
        ))
        self.next_updates.append(self.next_lock_state.expire_time)

    def _direct_take_lock(self):
        self.next_lock_state.datetime = self.current_time
        self.next_lock_state.owner = self.req_record.client
        self.next_lock_state.expire_time = self.current_time + timedelta(seconds=2.0)
        self.lock_notifications.append(LockNotificationRecord(
            client=self.next_lock_state.owner,
            lock_notification=LockNotification(state="TOOK", expire_time=self.next_lock_state.expire_time),
            datetime=self.current_time
        ))
        self.lock_notifications.append(LockNotificationRecord(
            client=self.lock_state.owner,
            lock_notification=LockNotification(state="RELEASED"),
            datetime=self.current_time
        ))
        self.next_updates.append(self.next_lock_state.expire_time)

    def _pending(self):
        self.next_lock_state.datetime = self.current_time
        self.next_lock_state.pending_clients.append(self.req_record.client)
        self.lock_notifications.append(LockNotificationRecord(
            client=self.next_lock_state.owner,
            lock_notification=LockNotification(state="PENDING"),
            datetime=self.current_time
        ))

    def _lock_req_move(self):
        if self.lock_state.owner == self.req_record.client and self.lock_state.expire_time > self.current_time:
            self._expand()
        else:
            if len(self.lock_state.pending_clients) == 0 and self.lock_state.expire_time <= self.current_time:
                self._direct_take_lock()
            elif self.req_record.client not in self.lock_state.pending_clients:
                self._pending()

    def _stop_pending(self):
        self.next_lock_state.datetime = self.current_time
        self.next_lock_state.pending_clients.remove(self.req_record.client)
        self.lock_notifications.append(LockNotificationRecord(
            client=self.req_record.client,
            lock_notification=LockNotification(state="STOPPED PENDING"),
            datetime=self.current_time
        ))

    def _no_op(self):
        self.lock_notifications.append(LockNotificationRecord(
            client=self.req_record.client,
            lock_notification=LockNotification(state="NOOP"),
            datetime=self.current_time
        ))

    def _unlock_req_move(self):
        if self.req_record.client == self.lock_state.owner:
            self._expire_move()
        elif self.req_record.client in self.lock_state.pending_clients:
            self._stop_pending()
        else:
            self._no_op()

    def take(self):
        self.next_lock_state = self.lock_state.copy()
        if self.req_record is None:
            if self.lock_state.expire_time <= self.current_time:
                self._expire_move()
        else:
            if self.req_record.command.intent == "LOCK":
                self._lock_req_move()
            elif self.req_record.command.intent == "UNLOCK":
                self._unlock_req_move()

    def get_next_lock_state(self) -> LockStateRecord:
        return self.next_lock_state

    def has_lock_state_changed(self) -> bool:
        return self.next_lock_state != self.lock_state

    def get_lock_notifications(self) -> list[LockNotificationRecord]:
        return self.lock_notifications


async def get_current_state() -> Optional[LockStateRecord]:
    res = await connectivity.mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME] \
        .find_one(sort=[("datetime", pymongo.DESCENDING)])
    return LockStateRecord.parse_obj(res) if res is not None else None


async def push_lock_state(lock_state: LockStateRecord):
    await connectivity.mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME].insert_one(
        lock_state.dict()
    )


async def push_notification_record(record: LockNotificationRecord):
    await connectivity.mongo_client[DATABASE_NAME][LOCK_NOTIFICATION_RECORD_COLLECTION_NAME].insert_one(
        record.dict()
    )


async def initialize_lock_state():
    await push_lock_state(
        LockStateRecord(owner="noone", pending_clients=[], datetime=datetime.now(), expire_time=datetime.now()))


async def transition(req_record: Optional[LockReqRecord], current_state: LockStateRecord, moment: datetime):
    transition_inst = Transition(current_state, req_record, moment)
    transition_inst.take()
    if transition_inst.has_lock_state_changed():
        await push_lock_state(transition_inst.get_next_lock_state())
    [await push_notification_record(notification) for notification in transition_inst.get_lock_notifications()]
    [await schedule_transition(moment) for moment in transition_inst.next_updates]


async def schedule_transition(moment: datetime):
    async def impl():
        await asyncio.sleep((moment - datetime.now()).total_seconds())
        current_state = await get_current_state()
        await transition(None, current_state, moment) if current_state is not None else None

    connectivity.coroutine_reqs.append(impl())


async def new_req_callback(req_record: LockReqRecord):
    logger.debug("got the callback")
    logger.debug(req_record)
    current_state = await get_current_state()
    if current_state is None:
        await initialize_lock_state()
        current_state = await get_current_state()
    await transition(req_record, current_state, req_record.datetime)


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
