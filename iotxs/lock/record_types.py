from datetime import datetime

from pydantic import BaseModel

from iotxs.msg_types import LockCommand, LockNotification

DATABASE_NAME = "iotxs"
LOCK_REQ_RECORD_COLLECTION_NAME = "lock_req_records"
LOCK_STATE_RECORD_COLLECTION_NAME = "lock_records"
LOCK_NOTIFICATION_RECORD_COLLECTION_NAME = "lock_notification_records"


class LockReqRecord(BaseModel):
    client: str
    command: LockCommand
    datetime: datetime


class LockStateRecord(BaseModel):
    owner_list: list[str]
    datetime: datetime
    expire_time: datetime


class LockNotificationRecord(BaseModel):
    client: str
    lock_notification: LockNotification
    datetime: datetime
