import signal
import asyncio
from loguru import logger
from pydantic import ValidationError

from . import data_types
from iotxs import msg_types


async def connection_callback(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    logger.info("got connection!")
    while not writer.is_closing() and not reader.at_eof():
        req_bytes = await reader.readline()
        try:
            req = data_types.SerialDeviceRequest.parse_raw(req_bytes[:-1])
            logger.debug("read {}".format(req))
            res = data_types.SerialDeviceResponse(id=req.id,
                                                  response=msg_types.DeviceResponse(state="SUCCESSFUL", data=""))
            logger.debug("write {}".format(res.json()))
            writer.write((res.json() + "\n").encode())
            await writer.drain()
        except ValidationError as e:
            logger.exception(e)


async def async_main():
    server = await asyncio.start_server(connection_callback, '127.0.0.1', 9995)

    async with server:
        logger.info("start serving")
        await server.serve_forever()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
