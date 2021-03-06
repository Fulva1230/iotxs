from iotxs import connectivity
import re
from datetime import datetime, timedelta
import asyncio
from .record_types import LockReqRecord, LockStateRecord
from ..msg_types import LockNotification, LockCommand
import time
import pymongo
from . import client_emulator


def test_datetime():
    print(type(datetime.now() + timedelta(seconds=3)))


def test_poll_new_record():
    connectivity.init()
    saved_records = []

    async def iterate_change_stream():
        try:
            async with connectivity.mongo_client["iotxs"]["lock_req_records"].watch() as change_stream:
                i = 0
                async for change in change_stream:
                    i += 1
                    saved_records.append(LockReqRecord.parse_obj(change['fullDocument']))
                    if i == 5:
                        await change_stream.close()
        except BaseException as e:
            print(e)
        connectivity.deinit()

    connectivity.coroutine_reqs.append(iterate_change_stream())
    connectivity.thread.join()
    assert len(saved_records) == 5


def test_basic_working():
    connectivity.init()
    saved_msgs: list[LockNotification] = []

    def notification_callback(client, userdata, msg):
        try:
            saved_msgs.append(LockNotification.parse_raw(msg.payload))
        except BaseException as e:
            print(e)

    async def setup_notification_callback():
        connectivity.mqtt_client.subscribe("iotxs/John/lock_notification")
        connectivity.mqtt_client.message_callback_add("iotxs/John/lock_notification", notification_callback)

    connectivity.coroutine_reqs.append(setup_notification_callback())

    async def publish_req():
        connectivity.mqtt_client.publish("iotxs/John/lock", LockCommand(intent="LOCK").json())

    connectivity.coroutine_reqs.append(publish_req())
    time.sleep(4.0)
    connectivity.deinit()
    connectivity.thread.join()
    assert len(saved_msgs) == 2
    assert saved_msgs[0].state == "TOOK"
    assert saved_msgs[1].state == "RELEASED"


def test_basic_pending_working():
    connectivity.init()
    saved_msgs: list[LockNotification] = []

    def notification_callback(client, userdata, msg):
        try:
            saved_msgs.append(LockNotification.parse_raw(msg.payload))
        except BaseException as e:
            print(e)

    async def setup_notification_callback():
        connectivity.mqtt_client.subscribe("iotxs/John/lock_notification")
        connectivity.mqtt_client.message_callback_add("iotxs/John/lock_notification", notification_callback)
        connectivity.mqtt_client.subscribe("iotxs/Alice/lock_notification")
        connectivity.mqtt_client.message_callback_add("iotxs/Alice/lock_notification", notification_callback)

    connectivity.coroutine_reqs.append(setup_notification_callback())

    async def publish_req():
        connectivity.mqtt_client.publish("iotxs/John/lock", LockCommand(intent="LOCK").json())
        await asyncio.sleep(1.0)
        connectivity.mqtt_client.publish("iotxs/Alice/lock", LockCommand(intent="LOCK").json())
        await asyncio.sleep(1.0)

    connectivity.coroutine_reqs.append(publish_req())
    time.sleep(5.0)
    connectivity.deinit()
    connectivity.thread.join()
    assert len(saved_msgs) == 5
    assert saved_msgs[0].state == "TOOK"
    assert saved_msgs[1].state == "PENDING"
    assert saved_msgs[2].state == "RELEASED"
    assert saved_msgs[3].state == "TOOK"
    assert saved_msgs[4].state == "RELEASED"


def test_pending_with_holding_working():
    connectivity.init()
    john_msgs: list[LockNotification] = []
    alice_msgs: list[LockNotification] = []

    def john_notification_callback(client, userdata, msg):
        try:
            john_msgs.append(LockNotification.parse_raw(msg.payload))
        except BaseException as e:
            print(e)

    def alice_notification_callback(client, userdata, msg):
        try:
            alice_msgs.append(LockNotification.parse_raw(msg.payload))
        except BaseException as e:
            print(e)

    async def setup_notification_callback():
        connectivity.mqtt_client.subscribe("iotxs/John/lock_notification")
        connectivity.mqtt_client.message_callback_add("iotxs/John/lock_notification", john_notification_callback)
        connectivity.mqtt_client.subscribe("iotxs/Alice/lock_notification")
        connectivity.mqtt_client.message_callback_add("iotxs/Alice/lock_notification", alice_notification_callback)

    connectivity.coroutine_reqs.append(setup_notification_callback())

    async def publish_req():
        connectivity.mqtt_client.publish("iotxs/John/lock", LockCommand(intent="LOCK").json())
        await asyncio.sleep(1.0)
        connectivity.mqtt_client.publish("iotxs/John/lock", LockCommand(intent="LOCK").json())
        connectivity.mqtt_client.publish("iotxs/Alice/lock", LockCommand(intent="LOCK").json())
        await asyncio.sleep(1.0)
        connectivity.mqtt_client.publish("iotxs/John/lock", LockCommand(intent="LOCK").json())

    connectivity.coroutine_reqs.append(publish_req())
    time.sleep(7.0)
    connectivity.deinit()
    connectivity.thread.join()
    assert len(john_msgs) == 4
    assert john_msgs[0].state == "TOOK"
    assert john_msgs[1].state == "HOLD"
    assert john_msgs[2].state == "HOLD"
    assert john_msgs[3].state == "RELEASED"
    assert len(alice_msgs) == 3
    assert alice_msgs[0].state == "PENDING"
    assert alice_msgs[1].state == "TOOK"
    assert alice_msgs[2].state == "RELEASED"


def test_get_latest_state():
    connectivity.init()
    latest_non_expire: list[LockStateRecord] = []

    async def task():
        try:
            await connectivity.mongo_client["iotxs"]["test_lock_records"].drop()
            await connectivity.mongo_client["iotxs"]["test_lock_records"].insert_many(
                [
                    LockStateRecord(owner_list=["John"], datetime=datetime(2020, 11, 2),
                                    expire_time=datetime(2020, 12, 2)).dict(),
                    LockStateRecord(owner_list=["John"], datetime=datetime(2020, 11, 3),
                                    expire_time=datetime(2020, 12, 3)).dict(),
                    LockStateRecord(owner_list=["John"], datetime=datetime(2020, 11, 1),
                                    expire_time=datetime(2020, 12, 1)).dict(),
                ]
            )
            res = await connectivity.mongo_client["iotxs"]["test_lock_records"] \
                .find_one(sort=[("datetime", pymongo.DESCENDING)])
            latest_non_expire.append(LockStateRecord.parse_obj(res))
        except BaseException as e:
            print(e)
        finally:
            connectivity.deinit()

    connectivity.coroutine_reqs.append(task())
    connectivity.thread.join()

    assert latest_non_expire[0] == LockStateRecord(owner_list=["John"], datetime=datetime(2020, 11, 3),
                                                   expire_time=datetime(2020, 12, 3))


def test_basic_unlock():
    connectivity.init()
    client = client_emulator.Client("Alice")

    async def task():
        client.lock()
        await asyncio.sleep(0.2)
        client.unlock()

    connectivity.coroutine_reqs.append(task())
    time.sleep(1.5)
    connectivity.deinit()
    connectivity.thread.join()
    assert client.received_msgs[0].state == "TOOK"
    assert client.received_msgs[1].state == "RELEASED"


def test_basic_stop_pending():
    connectivity.init()
    alice = client_emulator.Client("Alice")
    eren = client_emulator.Client("Eren")

    async def task():
        alice.lock()
        await asyncio.sleep(0.05)
        eren.lock()
        await asyncio.sleep(0.05)
        eren.unlock()
        await asyncio.sleep(0.05)
        alice.unlock()
        await asyncio.sleep(0.05)
        eren.unlock()

    connectivity.coroutine_reqs.append(task())
    time.sleep(1.5)
    connectivity.deinit()
    connectivity.thread.join()
    assert alice.received_msgs[0].state == "TOOK"
    assert alice.received_msgs[1].state == "RELEASED"
    assert eren.received_msgs[0].state == "PENDING"
    assert eren.received_msgs[1].state == "STOPPED PENDING"
    assert eren.received_msgs[2].state == "NOOP"


def test_general_case1():
    connectivity.init()
    alice = client_emulator.Client("Alice")
    eren = client_emulator.Client("Eren")

    async def task():
        alice.lock()
        await asyncio.sleep(0.05)
        eren.lock()
        await asyncio.sleep(0.05)
        alice.lock()
        await asyncio.sleep(0.05)
        alice.lock()
        await asyncio.sleep(0.05)
        eren.lock()
        await asyncio.sleep(0.05)
        alice.unlock()
        await asyncio.sleep(0.05)
        eren.lock()
        await asyncio.sleep(0.05)
        eren.unlock()

    connectivity.coroutine_reqs.append(task())
    time.sleep(1.5)
    connectivity.deinit()
    connectivity.thread.join()
    assert alice.received_msgs[0].state == "TOOK"
    assert alice.received_msgs[1].state == "HOLD"
    assert alice.received_msgs[2].state == "HOLD"
    assert alice.received_msgs[3].state == "RELEASED"
    assert eren.received_msgs[0].state == "PENDING"
    assert eren.received_msgs[1].state == "NOOP"
    assert eren.received_msgs[2].state == "TOOK"
    assert eren.received_msgs[3].state == "HOLD"
    assert eren.received_msgs[4].state == "RELEASED"


def test_soft_pressure_test():
    connectivity.init()
    client = client_emulator.Client("Alice")

    async def task():
        for i in range(20):
            client.lock()
            await asyncio.sleep(0.05)
        client.unlock()

    connectivity.coroutine_reqs.append(task())
    time.sleep(2.0)
    connectivity.deinit()
    connectivity.thread.join()
    assert len(client.received_msgs) == 21
    assert client.received_msgs[0].state == "TOOK"
    for i in range(1, 20):
        assert client.received_msgs[i].state == "HOLD"
    assert client.received_msgs[20].state == "RELEASED"


def test_soft_pressure_for_long_test():
    connectivity.init()
    client = client_emulator.Client("Alice")
    secs = 60

    async def task():
        for i in range(20 * secs):
            client.lock()
            await asyncio.sleep(0.05)
        client.unlock()

    connectivity.coroutine_reqs.append(task())
    time.sleep(secs + 10)
    connectivity.deinit()
    connectivity.thread.join()
    assert len(client.received_msgs) == 20 * secs + 1
    assert client.received_msgs[0].state == "TOOK"
    for i in range(1, 20 * secs):
        assert client.received_msgs[i].state == "HOLD"
    assert client.received_msgs[20 * secs].state == "RELEASED"


def test_pressure_test():
    connectivity.init()
    client = client_emulator.Client("Alice")

    async def task():
        for i in range(50):
            client.lock()
            await asyncio.sleep(0.02)
        client.unlock()

    connectivity.coroutine_reqs.append(task())
    time.sleep(1.5)
    connectivity.deinit()
    connectivity.thread.join()
    assert len(client.received_msgs) == 51
    assert client.received_msgs[0].state == "TOOK"
    for i in range(1, 50):
        assert client.received_msgs[i].state == "HOLD"
    assert client.received_msgs[50].state == "RELEASED"


def test_time_delta():
    assert ((datetime(2021, 12, 30, 11, 11, 12) - datetime(2021, 12, 30, 11, 11, 15)).total_seconds()) == -3.0
