import asyncio
import heapq
from datetime import datetime, timedelta

from iotxs import connectivity
from .record_types import LockReqRecord, DATABASE_NAME, LOCK_STATE_RECORD_COLLECTION_NAME, \
    LOCK_REQ_RECORD_COLLECTION_NAME, LockStateRecord, LockNotificationRecord, LOCK_NOTIFICATION_RECORD_COLLECTION_NAME
from pydantic import ValidationError
from logging import Logger
from typing import Optional, Callable, Awaitable, Coroutine, Protocol, ClassVar
from iotxs.msg_types import LockNotification
from anyio import sleep, create_task_group

life_state = False
logger: Optional[Logger] = None
deinit_listeners: list[Coroutine] = []


class StateAgent(Protocol):
    async def push_lock_state(self, lock_state_record: LockStateRecord):
        ...

    async def get_current_state(self) -> LockStateRecord:
        ...

    async def push_lock_notification(self, lock_notification_record: LockNotificationRecord):
        ...


class EventAgent(Protocol):
    async def next_lock_req(self) -> LockReqRecord:
        ...

    async def listen_lock_req_task(self):
        ...


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
        self.lock_notifications.append(LockNotificationRecord(
            client=self.lock_state.owner_list[0],
            lock_notification=LockNotification(state="RELEASED"),
            datetime=self.current_time
        ))
        self.next_lock_state.owner_list.pop(0)
        if len(self.next_lock_state.owner_list) > 0:
            self.next_lock_state.expire_time = self.current_time + timedelta(seconds=2.0)
            self.lock_notifications.append(LockNotificationRecord(
                client=self.next_lock_state.owner_list[0],
                lock_notification=LockNotification(state="TOOK", expire_time=self.next_lock_state.expire_time),
                datetime=self.current_time
            ))
            self.next_updates.append(self.current_time + timedelta(seconds=2.0))
        else:
            self.next_lock_state.expire_time = self.current_time

    def _expand(self):
        self.next_lock_state.datetime = self.current_time
        self.next_lock_state.expire_time = self.current_time + timedelta(seconds=2.0)
        self.lock_notifications.append(LockNotificationRecord(
            client=self.next_lock_state.owner_list[0],
            lock_notification=LockNotification(state="HOLD", expire_time=self.next_lock_state.expire_time),
            datetime=self.current_time
        ))
        self.next_updates.append(self.next_lock_state.expire_time)

    def _direct_take_lock(self):
        self.next_lock_state.datetime = self.current_time
        self.next_lock_state.owner_list.append(self.req_record.client)
        self.next_lock_state.expire_time = self.current_time + timedelta(seconds=2.0)
        self.lock_notifications.append(LockNotificationRecord(
            client=self.req_record.client,
            lock_notification=LockNotification(state="TOOK", expire_time=self.next_lock_state.expire_time),
            datetime=self.current_time
        ))
        self.next_updates.append(self.next_lock_state.expire_time)

    def _pending(self):
        self.next_lock_state.datetime = self.current_time
        self.next_lock_state.owner_list.append(self.req_record.client)
        self.lock_notifications.append(LockNotificationRecord(
            client=self.req_record.client,
            lock_notification=LockNotification(state="PENDING"),
            datetime=self.current_time
        ))

    def _lock_req_move(self):
        if len(self.lock_state.owner_list) > 0:
            if self.lock_state.owner_list[0] == self.req_record.client:
                self._expand()
            elif self.req_record.client not in self.lock_state.owner_list:
                self._pending()
            else:
                self._no_op()
        else:
            self._direct_take_lock()

    def _stop_pending(self):
        self.next_lock_state.datetime = self.current_time
        self.next_lock_state.owner_list.remove(self.req_record.client)
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
        if len(self.lock_state.owner_list) > 0:
            if self.req_record.client == self.lock_state.owner_list[0]:
                self._expire_move()
            elif self.req_record.client in self.lock_state.owner_list:
                self._stop_pending()
            else:
                self._no_op()
        else:
            self._no_op()

    def take(self):
        self.next_lock_state = self.lock_state.copy()
        if self.req_record is None:
            if self.lock_state.expire_time <= self.current_time and len(self.lock_state.owner_list) > 0:
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


class Coordinator:
    _state_agent: StateAgent
    _event_agent: EventAgent
    _update_moments: list[datetime]

    def __init__(self, state_agent: StateAgent,
                 event_agent: EventAgent):
        self._state_agent = state_agent
        self._event_agent = event_agent
        self._update_moments = []

    async def transition(self, transition_inst: Transition):
        transition_inst.take()
        if transition_inst.has_lock_state_changed():
            await self._state_agent.push_lock_state(transition_inst.get_next_lock_state())
        [await self._state_agent.push_lock_notification(notification) for notification in
         transition_inst.get_lock_notifications()]
        for moment in transition_inst.next_updates:
            heapq.heappush(self._update_moments, moment)

    async def process_self_update(self):
        while True:
            try:
                moment = heapq.heappop(self._update_moments)
                current_time = datetime.now()
                wait_time = (moment - current_time).total_seconds()
                while wait_time > 0:
                    await asyncio.sleep(wait_time)
                    current_time = datetime.now()
                    wait_time = (moment - current_time).total_seconds()
                current_state = await self._state_agent.get_current_state()
                await self.transition(Transition(current_state, None, current_time))
            except IndexError:
                await asyncio.sleep(0.01)

    async def process_lock_reqs(self):
        while True:
            lock_req = await self._event_agent.next_lock_req()
            lock_state = await self._state_agent.get_current_state()
            moment = datetime.now()
            await self.transition(Transition(lock_state, lock_req, moment))

    async def task(self):
        async with create_task_group() as tg:
            tg.start_soon(self.process_lock_reqs)
            tg.start_soon(self.process_self_update)
            tg.start_soon(self._event_agent.listen_lock_req_task)
