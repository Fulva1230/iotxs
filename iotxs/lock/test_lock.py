from iotxs.lock import mqtt_listener
from iotxs import connectivity
import re
from datetime import datetime, timedelta
import asyncio
from .record_types import LockReqRecord


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
