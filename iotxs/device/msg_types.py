from iotxs import msg_types
from pydantic import BaseModel


class SerialDeviceRequest(BaseModel):
    _id: int
    device: str
    request: msg_types.DeviceRequest


class SerialDeviceResponse(BaseModel):
    respnose: msg_types.DeviceResponse
    _id: int
