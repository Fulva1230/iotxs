import threading

from iotxs.lock import mqtt_facer
from iotxs import connectivity
import re
from datetime import datetime, timedelta
import asyncio
from .record_types import LockReqRecord, LockStateRecord
from ..msg_types import LockNotification, LockCommand
import time
import pymongo


def test_re_pattern():
    assert re.search(mqtt_listener.CLIENT_NAME_PATTERN, "jiowe/wjiofew/jfoiwejf/wioefj").group(1) == "wjiofew"


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
        connectivity.mqtt_client.subscribe("iotxs/+/lock_notification")
        connectivity.mqtt_client.message_callback_add("iotxs/+/lock_notification", notification_callback)

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
    assert len(saved_msgs) == 4
    assert saved_msgs[0].state == "TOOK"
    assert saved_msgs[1].state == "PENDING"
    assert saved_msgs[2].state == "RELEASED"
    assert saved_msgs[3].state == "TOOK"
    assert saved_msgs[4].state == "RELEASED"

def test_get_current_state_with_nothing():
    connectivity.init()
    latest_non_expire: list[LockStateRecord] = []

    async def task():
        try:
            await connectivity.mongo_client["iotxs"]["test_lock_records"].drop()
            await connectivity.mongo_client["iotxs"]["test_lock_records"].insert_many(
                [
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 1),
                                    expire_time=datetime(2020, 12, 1)).dict(),
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 2),
                                    expire_time=datetime(2020, 12, 2)).dict(),
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 3),
                                    expire_time=datetime(2020, 12, 3)).dict()
                ]
            )
            res = await connectivity.mongo_client["iotxs"]["test_lock_records"] \
                .find_one({"expire_time": {"$gt": datetime(2020, 12, 3)}}, sort=[("datetime", pymongo.DESCENDING)])
            latest_non_expire.append(LockStateRecord.parse_obj(res))
        except BaseException as e:
            print(e)
        finally:
            connectivity.deinit()

    connectivity.coroutine_reqs.append(task())
    connectivity.thread.join()

    assert len(latest_non_expire) == 0


def test_get_latest_state():
    connectivity.init()
    latest_non_expire: list[LockStateRecord] = []

    async def task():
        try:
            await connectivity.mongo_client["iotxs"]["test_lock_records"].drop()
            await connectivity.mongo_client["iotxs"]["test_lock_records"].insert_many(
                [
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 2),
                                    expire_time=datetime(2020, 12, 2)).dict(),
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 3),
                                    expire_time=datetime(2020, 12, 3)).dict(),
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 1),
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

    assert latest_non_expire[0] == LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 3),
                                                   expire_time=datetime(2020, 12, 3))


def test_get_current_state():
    connectivity.init()
    latest_non_expire: list[LockStateRecord] = []

    async def task():
        try:
            await connectivity.mongo_client["iotxs"]["test_lock_records"].drop()
            await connectivity.mongo_client["iotxs"]["test_lock_records"].insert_many(
                [
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 1),
                                    expire_time=datetime(2020, 12, 1)).dict(),
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 2),
                                    expire_time=datetime(2020, 12, 2)).dict(),
                    LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 3),
                                    expire_time=datetime(2020, 12, 3)).dict()
                ]
            )
            res = await connectivity.mongo_client["iotxs"]["test_lock_records"] \
                .find_one({"expire_time": {"$gt": datetime(2020, 12, 1)}}, sort=[("datetime", pymongo.DESCENDING)])
            latest_non_expire.append(LockStateRecord.parse_obj(res))
        except BaseException as e:
            print(e)
        finally:
            connectivity.deinit()

    connectivity.coroutine_reqs.append(task())
    connectivity.thread.join()

    assert latest_non_expire[0] == LockStateRecord(owner="John", pending_clients=[], datetime=datetime(2020, 11, 3),
                                                   expire_time=datetime(2020, 12, 3))
