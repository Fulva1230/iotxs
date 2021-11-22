from datetime import datetime
from typing import List, Optional, Literal
from pydantic import BaseModel


class DatetimeContent(BaseModel):
    content: str
    datetime: datetime


class LockCommand(BaseModel):
    intent: Literal["LOCK", "UNLOCK"]


class LockNotification(BaseModel):
    state: Literal["PENDING", "TOOK", "HOLD", "RELEASED", "STOP PENDING", "NOOP"]
    expire_time: Optional[datetime] = None
