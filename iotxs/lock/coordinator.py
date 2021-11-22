from iotxs import connectivity
from .record_types import LockReqRecord
from pydantic import ValidationError
from logging import Logger
from typing import Optional

life_state = False
logger: Optional[Logger] = None


async def new_req_callback(req_record: LockReqRecord):
    logger.debug("got the callback")
    logger.debug(req_record)


async def setup_new_req_callback():
    try:
        async with connectivity.mongo_client["iotxs"]["lock_req_records"].watch() as change_stream:
            i = 0
            async for change in change_stream:
                i += 1
                await new_req_callback(LockReqRecord.parse_obj(change['fullDocument']))
                if not life_state:
                    await change_stream.close()
    except ValidationError as e:
        logger.exception(e)


def init():
    global life_state
    life_state = True
    connectivity.coroutine_reqs.append(setup_new_req_callback())


def deinit():
    global life_state
    life_state = False
