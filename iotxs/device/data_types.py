from iotxs import msg_types
from pydantic import BaseModel
from datetime import datetime
from uuid import UUID


class DeviceRequestRecord(BaseModel):
    id: UUID
    client: str
    device: str
    request: msg_types.DeviceRequest
    datetime: datetime


class DeviceResponseRecord(BaseModel):
    id: UUID
    datetime: datetime
    response: msg_types.DeviceResponse


class SerialDeviceRequest(BaseModel):
    id: UUID
    has_lock: bool
    device: str
    request: msg_types.DeviceRequest


class SerialDeviceResponse(BaseModel):
    id: UUID
    response: msg_types.DeviceResponse
