import asyncio

from iotxs.io.io import init_mongo_client


async def async_main():
    mongo_client_generator = init_mongo_client()
    mongo_client = await mongo_client_generator.__anext__()
    await mongo_client["iotxs"]["device_reqs"].drop()
    await mongo_client["iotxs"]["device_ress"].drop()
    await mongo_client["iotxs"].create_collection("device_reqs", capped=True, size=1024 * 1024)
    await mongo_client["iotxs"]["device_reqs"].create_index("id")
    await mongo_client["iotxs"].create_collection("device_ress", capped=True, size=1024 * 1024)
    await mongo_client["iotxs"]["device_ress"].create_index("id")
    async for mongo_client in mongo_client_generator: pass


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
