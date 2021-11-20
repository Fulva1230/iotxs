from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel

class DatetimeContent(BaseModel):
    content: str
    datetime: datetime