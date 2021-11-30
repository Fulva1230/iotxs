import asyncio
import signal
from anyio import create_task_group
from loguru import logger

from dependency_injector import containers, providers
from iotxs.io.io import init_mongo_client, init_mqtt_client
from . import agents
from . import server


class Container(containers.DeclarativeContainer):
    mqtt_client = providers.Resource(init_mqtt_client)
    mongo_client = providers.Resource(init_mongo_client)
    serial_agent = providers.Resource(agents.init_serial_agent, host="127.0.0.1", port=9995)
    persistence_agent = providers.Factory(agents.PersistenceAgentImpl, mongo_client=mongo_client)
    lock_agent = providers.Factory(agents.LockAgentImpl, mongo_client=mongo_client)
    server = providers.Factory(server.Server, mqtt_client=mqtt_client, serial_agent=serial_agent,
                               persistence_agent=persistence_agent, lock_agent=lock_agent)


async def async_main():
    container = Container()
    await container.init_resources()
    server = await container.server()
    async with create_task_group() as tg:
        tg.start_soon(server.task)

        def shutdown_handle(signal, frame):
            tg.cancel_scope.cancel()

        signal.signal(signal.SIGINT, shutdown_handle)

        logger.info("Started successfully")
    await container.shutdown_resources()
    logger.info("Successfully shutdown")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
