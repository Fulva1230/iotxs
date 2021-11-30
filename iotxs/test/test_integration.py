import asyncio
from iotxs.io.io import init_mqtt_client
from iotxs import msg_types


def test_lock_put():
    async def impl():
        mqtt_client_generator = init_mqtt_client()
        mqtt_client = await mqtt_client_generator.__anext__()
        received_device_responses: list[msg_types.DeviceResponse] = []

        def received_device_response_callback(client, userdata, msg):
            received_device_responses.append(msg_types.DeviceResponse.parse_raw(msg.payload))

        mqtt_client.subscribe("iotxs/John/device/motor1/res")
        mqtt_client.message_callback_add("iotxs/John/device/motor1/res", received_device_response_callback)
        mqtt_client.publish("iotxs/John/lock", msg_types.LockCommand(intent="LOCK").json())
        await asyncio.sleep(0.3)
        mqtt_client.publish("iotxs/John/device/motor1", msg_types.DeviceRequest(intent="PUT", data="").json())
        await asyncio.sleep(0.3)
        assert len(received_device_responses) == 1
        assert received_device_responses[0] == msg_types.DeviceResponse(state="SUCCESSFUL", data="")
        async for i in mqtt_client_generator: pass

    asyncio.run(impl())
