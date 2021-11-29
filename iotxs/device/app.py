import asyncio
from typing import Protocol
from paho.mqtt.client import Client as MqttClient
import attr

SUBSCRIBE_TOPIC = "iotxs/+/device/+"
PUBLISH_TOPIC = "iotxs/{client}/device/{device}/res"


class SerialAgent(Protocol):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter


@attr.s
class DeviceServer:
    mqtt_client: MqttClient = attr.ib(kw_only=True)

    async def task(self):
        ...

    def msg_callback(self, client, userdata, msg):
        ...

    async def subscribe_to_topics(self):
        self.mqtt_client.subscribe(SUBSCRIBE_TOPIC, 0)


async def async_main():
    ...


def main():
    ...


if __name__ == "__main__":
    main()
