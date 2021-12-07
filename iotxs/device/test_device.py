import asyncio
import uuid
from datetime import datetime

from iotxs.io.io import init_mongo_client
from iotxs.device.agents import PersistenceAgentImpl
from iotxs.device.data_types import DeviceRequestRecord, DeviceResponseRecord, SerialDeviceRequest
from iotxs.msg_types import DeviceRequest, DeviceResponse

from dependency_injector import containers, providers


class TestPersistenceAgentContainer(containers.DeclarativeContainer):
    mongo_client = providers.Resource(init_mongo_client)
    persistence_agent = providers.Factory(PersistenceAgentImpl, mongo_client=mongo_client)


def test_persistence_agent():
    async def impl():
        container = TestPersistenceAgentContainer()
        await container.init_resources()
        persistence_agent = await container.persistence_agent()
        req_record = DeviceRequestRecord(id=uuid.uuid4(), client="John", device="motor1",
                                         request=DeviceRequest(intent="GET", data=""), datetime=datetime(1, 1, 1))
        await persistence_agent.save_device_request(req_record)
        req_record_from_database = await persistence_agent.get_device_request(req_record.id)
        assert req_record == req_record_from_database

        res_record = DeviceResponseRecord(id=uuid.uuid4(), datetime=datetime(1, 1, 1),
                                          response=DeviceResponse(state="SUCCESSFUL", data=""))
        await persistence_agent.save_device_response(res_record)
        res_record_from_database = await persistence_agent.get_device_response(res_record.id)
        assert res_record == res_record_from_database
        await container.shutdown_resources()

    asyncio.run(impl())


def test_tcp_server_emulator():
    async def impl():
        (reader, writer) = await asyncio.open_connection("localhost", 9995)
        req = SerialDeviceRequest(id=uuid.uuid4(), has_lock=True, device="motor1", request=DeviceRequest(intent="GET", data=""))
        print("write {}".format(req.json()))
        writer.write((req.json() + '\n').encode())
        await writer.drain()
        readbytes = await reader.readline()
        print(readbytes)
        writer.close()

    asyncio.run(impl())
