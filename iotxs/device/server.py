import asyncio
import uuid
from collections import deque
from typing import Protocol
from uuid import UUID

from anyio import create_task_group
from paho.mqtt.client import Client as MqttClient
import attr
from pydantic import ValidationError

from iotxs.device.data_types import DeviceRequestRecord, SerialDeviceRequest, SerialDeviceResponse, DeviceResponseRecord
from iotxs.lock.record_types import LockStateRecord
from iotxs.msg_types import DeviceRequest
from datetime import datetime

from loguru import logger

SUBSCRIBE_TOPIC = "iotxs/+/device/+"
PUBLISH_TOPIC = "iotxs/{client}/device/{device}/res"


class SerialAgent(Protocol):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class PersistenceAgent(Protocol):
    async def save_device_request(self, req: DeviceRequestRecord): ...

    async def get_device_request(self, id: UUID) -> DeviceRequestRecord: ...

    async def save_device_response(self, res: DeviceResponseRecord): ...

    async def get_device_response(self, id: UUID) -> DeviceResponseRecord: ...


class LockAgent(Protocol):
    async def get_current_state(self) -> LockStateRecord: ...

    async def lock_state_listening(self): ...


@attr.s
class Server:
    mqtt_client: MqttClient = attr.ib(kw_only=True)
    serial_agent: SerialAgent = attr.ib(kw_only=True)
    persistence_agent: PersistenceAgent = attr.ib(kw_only=True)
    lock_agent: LockAgent = attr.ib(kw_only=True)
    received_reqs: deque[DeviceRequestRecord] = attr.ib(default=attr.Factory(deque))

    async def request_to_serial(self, req: SerialDeviceRequest):
        logger.debug("write to serial {}".format(req.json()))
        self.serial_agent.writer.write((req.json() + '\n').encode())
        await self.serial_agent.writer.drain()

    async def has_lock(self, client: str) -> bool:
        lock = await self.lock_agent.get_current_state()
        if len(lock.owner_list) > 0:
            return lock.owner_list[0] == client
        else:
            return False

    async def process_received_reqs(self):
        while True:
            try:
                req = self.received_reqs.popleft()
                await self.persistence_agent.save_device_request(req)
                lock_state = await self.has_lock(req.client)
                serial_req = SerialDeviceRequest(has_lock=lock_state,
                                                 device=req.device,
                                                 request=req.request,
                                                 id=req.id)

                await self.request_to_serial(serial_req)
            except IndexError:
                await asyncio.sleep(0.01)
            except ValidationError as e:
                logger.debug(e)

    async def next_res(self) -> SerialDeviceResponse:
        while True:
            if not self.serial_agent.reader.at_eof():
                res_bytes = await self.serial_agent.reader.readline()
                try:
                    return SerialDeviceResponse.parse_raw(res_bytes)
                except ValidationError:
                    ...
            else:
                await asyncio.sleep(0.1)

    async def process_received_ress(self):
        while True:
            res = await self.next_res()
            req = await self.persistence_agent.get_device_request(res.id)
            await self.persistence_agent.save_device_response(
                DeviceResponseRecord(id=res.id, datetime=datetime.now(), response=res.response))
            self.mqtt_client.publish(PUBLISH_TOPIC.format(client=req.client, device=req.device), res.response.json())

    async def task(self):
        await self.subscribe_to_topics()
        try:
            async with create_task_group() as tg:
                tg.start_soon(self.process_received_ress)
                tg.start_soon(self.process_received_reqs)
                tg.start_soon(self.lock_agent.lock_state_listening)
        finally:
            await self.unsubscribe_to_topics()

    def msg_callback(self, client, userdata, msg):
        (_, client_str, _, device_str) = msg.topic.split('/')
        try:
            logger.debug("got msg{}".format(msg.payload))
            self.received_reqs.append(
                DeviceRequestRecord(client=client_str, device=device_str, datetime=datetime.now(),
                                    request=DeviceRequest.parse_raw(msg.payload), id=uuid.uuid4())
            )
        except ValidationError:
            ...

    async def subscribe_to_topics(self):
        self.mqtt_client.subscribe(SUBSCRIBE_TOPIC, 0)
        self.mqtt_client.message_callback_add(SUBSCRIBE_TOPIC, self.msg_callback)

    async def unsubscribe_to_topics(self):
        self.mqtt_client.unsubscribe(SUBSCRIBE_TOPIC, 0)
        self.mqtt_client.message_callback_remove(SUBSCRIBE_TOPIC)
