import asyncio
from uuid import UUID
from motor.motor_asyncio import AsyncIOMotorClient

import attr

from iotxs.device.data_types import DeviceRequestRecord, DeviceResponseRecord


@attr.s
class SerialAgentImpl:
    reader: asyncio.StreamReader = attr.ib(kw_only=True)
    writer: asyncio.StreamWriter = attr.ib(kw_only=True)


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
