import asyncio
from collections import deque
from typing import Protocol
from paho.mqtt.client import Client as MqttClient
import attr
from pydantic import ValidationError

from iotxs.device.data_types import DeviceRequestRecord, SerialDeviceRequest, SerialDeviceResponse
from iotxs.lock.record_types import LockStateRecord
from iotxs.msg_types import DeviceRequest
from datetime import datetime

SUBSCRIBE_TOPIC = "iotxs/+/device/+"
PUBLISH_TOPIC = "iotxs/{client}/device/{device}/res"


class SerialAgent(Protocol):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


class LockAgent(Protocol):
    async def get_current_state(self) -> LockStateRecord: ...

    async def lock_state_listening(self): ...


@attr.s
class DeviceServer:
    mqtt_client: MqttClient = attr.ib(kw_only=True)
    serial_agent: SerialAgent = attr.ib(kw_only=True)
    lock_agent: LockAgent = attr.ib(kw_only=True)
    received_reqs: deque[DeviceRequestRecord] = attr.ib(default=attr.Factory(deque))
    wait_for_response_msgs: dict[int, DeviceRequestRecord] = attr.ib(default=attr.Factory(dict))
    received_ress: deque[DeviceRequestRecord] = attr.ib(default=attr.Factory(deque))
    received_reqs_seq: int = attr.ib(default=0)

    async def request_to_serial(self, req: SerialDeviceRequest):
        self.serial_agent.writer.write(req.json().encode())

    async def has_lock(self, client: str) -> bool:
        lock = await self.lock_agent.get_current_state()
        return lock.owner_list[0] == client

    async def process_received_reqs(self):
        while True:
            try:
                req = self.received_reqs.popleft()
                serial_req = SerialDeviceRequest(has_lock=await self.has_lock(req.client),
                                        device=req.device,
                                        request=req.request,
                                        id=self.received_reqs_seq)
                await self.request_to_serial(serial_req)
                self.wait_for_response_msgs[serial_req.id] = req
                self.received_reqs_seq += 1
            except IndexError:
                await asyncio.sleep(0.01)

    async def next_res(self) -> SerialDeviceResponse:
        while True:
            res_bytes = await self.serial_agent.reader.readline()
            try:
                return SerialDeviceResponse.parse_raw(res_bytes)
            except ValidationError:
                ...

    async def process_received_res(self):
        while True:
            res = await self.next_res()

    async def task(self):
        ...

    def msg_callback(self, client, userdata, msg):
        (_, client_str, _, device_str) = msg.topic.split('/')
        try:
            self.received_reqs.append(
                DeviceRequestRecord(client=client_str, device=device_str, datetime=datetime.now(),
                                    request=DeviceRequest.parse_raw(msg.payload))
            )
        except ValidationError:
            ...

    async def subscribe_to_topics(self):
        self.mqtt_client.subscribe(SUBSCRIBE_TOPIC, 0)
        self.mqtt_client.message_callback_add(SUBSCRIBE_TOPIC, self.msg_callback)


async def async_main():
    ...


def main():
    ...


if __name__ == "__main__":
    main()
