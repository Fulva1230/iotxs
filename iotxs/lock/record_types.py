from datetime import datetime

from pydantic import BaseModel

from iotxs.msg_types import LockCommand

DATABASE_NAME = "iotxs"
LOCK_REQ_RECORD_COLLECTION_NAME = "lock_req_records"


class LockReqRecord(BaseModel):
    client: str
    command: LockCommand
    datetime: datetime
