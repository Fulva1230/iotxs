import asyncio
from datetime import datetime
from typing import Optional
from uuid import UUID

import pymongo
from motor.motor_asyncio import AsyncIOMotorClient

import attr
from pydantic import ValidationError

from iotxs.config import DATABASE_NAME, LOCK_STATE_RECORD_COLLECTION_NAME
from iotxs.device.data_types import DeviceRequestRecord, DeviceResponseRecord
from iotxs.lock.record_types import LockStateRecord


@attr.s
class SerialAgentImpl:
    reader: asyncio.StreamReader = attr.ib(kw_only=True)
    writer: asyncio.StreamWriter = attr.ib(kw_only=True)


@attr.s
class LockAgentImpl:
    _mongo_client: AsyncIOMotorClient = attr.ib(kw_only=True)
    _cached_state: Optional[LockStateRecord] = attr.ib(default=None)

    async def get_current_state(self) -> LockStateRecord:
        if self._cached_state is None:
            self._cached_state = await self.manual_get_state()
        return self._cached_state

    async def manual_get_state(self) -> LockStateRecord:
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

    async def lock_state_listening(self):
        async with self._mongo_client[DATABASE_NAME][LOCK_STATE_RECORD_COLLECTION_NAME].watch() as change_stream:
            async for change in change_stream:
                try:
                    self._cached_state = LockStateRecord.parse_obj(change["fullDocument"])
                except ValidationError:
                    ...


@attr.s
class PersistenceAgentImpl:
    mongo_client: AsyncIOMotorClient = attr.ib(kw_only=True)

    async def save_device_request(self, req: DeviceRequestRecord):
        await self.mongo_client["iotxs"]["device_reqs"].insert_one(req.dict())

    async def get_device_request(self, id: UUID) -> DeviceRequestRecord:
        req = await self.mongo_client["iotxs"]["device_reqs"].find_one({"id": id})
        return DeviceRequestRecord.parse_obj(req)

    async def get_device_response(self, id: UUID) -> DeviceResponseRecord:
        res = await self.mongo_client["iotxs"]["device_ress"].find_one({"id": id})
        return DeviceResponseRecord.parse_obj(res)

    async def save_device_response(self, res: DeviceResponseRecord):
        await self.mongo_client["iotxs"]["device_ress"].insert_one(res.dict())


async def init_serial_agent(host: str, port: int):
    (reader, writer) = await asyncio.open_connection(host, port)
    yield SerialAgentImpl(reader=reader, writer=writer)
    writer.close()
    await writer.wait_closed()
