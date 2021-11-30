from datetime import datetime

from pydantic import BaseModel

from iotxs.msg_types import LockCommand, LockNotification


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
