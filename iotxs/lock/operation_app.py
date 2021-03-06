import anyio
from anyio import create_task_group
from dependency_injector import containers, providers
from loguru import logger
from iotxs.io.io import init_mongo_client
from iotxs.lock.coordinator import Coordinator
from iotxs.lock.agents import EventAgentImpl, StateAgentImpl
import asyncio
import signal


class Container(containers.DeclarativeContainer):
    mongo_client = providers.Resource(init_mongo_client)
    event_agent = providers.Factory(EventAgentImpl, mongo_client)
    state_agent = providers.Factory(StateAgentImpl, mongo_client)
    lock_coordinator = providers.Factory(Coordinator, state_agent=state_agent, event_agent=event_agent)


async def init_terminate_callback(cancel_scope: anyio.CancelScope):
    def handler(signum, frame):
        logger.info("Received Ctrl+C")
        cancel_scope.cancel()

    signal.signal(signal.SIGINT, handler)


def main():
    async def impl():
        container = Container()
        await container.init_resources()
        coordinator = await container.lock_coordinator()

        try:
            logger.info("started")
            async with create_task_group() as tg:
                tg.start_soon(coordinator.task)
                await init_terminate_callback(tg.cancel_scope)

        finally:
            logger.info("cleaning up")
            await container.shutdown_resources()
            logger.info("cleaned up")

    asyncio.run(impl())


if __name__ == "__main__":
    main()
