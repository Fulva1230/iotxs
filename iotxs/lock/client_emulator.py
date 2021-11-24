from .. import connectivity
from .. import msg_types
from pydantic import ValidationError


class Client:
    name: str
    received_msgs: list[msg_types.LockNotification]

    def __init__(self, name: str):
        self.name = name
        self.received_msgs = []
        connectivity.coroutine_reqs.append(self.setup_subscription())

    async def setup_subscription(self):
        connectivity.mqtt_client.subscribe("iotxs/{name}/lock_notification".format(name=self.name))
        connectivity.mqtt_client.message_callback_add("iotxs/{name}/lock_notification".format(name=self.name),
                                                      self.notification_callback)

    def notification_callback(self, client, userdata, msg):
        try:
            self.received_msgs.append(msg_types.LockNotification.parse_raw(msg.payload))
        except ValidationError as e:
            print(e)

    async def _publish(self, command: msg_types.LockCommand):
        connectivity.mqtt_client.publish("iotxs/{name}/lock".format(name=self.name),
                                         command.json())

    def lock(self):
        connectivity.coroutine_reqs.append(self._publish(msg_types.LockCommand(intent="LOCK")))

    def unlock(self):
        connectivity.coroutine_reqs.append(self._publish(msg_types.LockCommand(intent="UNLOCK")))
