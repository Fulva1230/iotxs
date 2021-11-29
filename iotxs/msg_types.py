from datetime import datetime
from typing import List, Optional, Literal
from pydantic import BaseModel


class DatetimeContent(BaseModel):
    content: str
    datetime: datetime


class LockCommand(BaseModel):
    intent: Literal["LOCK", "UNLOCK"]


class LockNotification(BaseModel):
    state: Literal["STARTED PENDING", "STOPPED PENDING", "PENDING", "TOOK", "HOLD", "RELEASED", "NOOP"]
    expire_time: Optional[datetime] = None


class DeviceRequest(BaseModel):
    intent: Literal["GET", "PUT"]
    data: str


class DeviceResponse(BaseModel):
    state: Literal["FAILED", "SUCCESSFUL"]
    data: str
