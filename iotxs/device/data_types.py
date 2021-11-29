from iotxs import msg_types
from pydantic import BaseModel
from datetime import datetime


class DeviceRequestRecord(BaseModel):
    client: str
    device: str
    request: msg_types.DeviceRequest
    datetime: datetime


class SerialDeviceRequest(BaseModel):
    id: int
    has_lock: bool
    device: str
    request: msg_types.DeviceRequest


class SerialDeviceResponse(BaseModel):
    id: int
    respnose: msg_types.DeviceResponse
