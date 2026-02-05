import asyncio
import itertools
import logging
from functools import partial
import uvloop
from proxy.config import load_config

from proxy.upstream_pool import UpstreamConnPool
from proxy.timeouts import Timeouts
from proxy.client_handler import handle_client
from proxy.logger import setup_logging


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

#setup_logging("logging.conf")
logger = logging.getLogger("")

class UpstreamPool:
    def __init__(self, backends):
        self.backends = backends
        self._rr = itertools.cycle(backends)

    def next(self):
        return next(self._rr)


async def main():
    config = load_config("app_conf.yml")
    timeouts = Timeouts(config.timeouts)

    backends = [
        (u.host, u.port)
        for u in config.upstreams
    ]

    pool = UpstreamPool(backends)
    up_pool = UpstreamConnPool(backends)

    client_sem = asyncio.Semaphore(
        config.limits.max_client_conns
    )

    upstream_limits = {
        b: asyncio.Semaphore(
            config.limits.max_conns_per_upstream
        )
        for b in backends
    }

    server = await asyncio.start_server(
        partial(
            handle_client,
            pool=pool,
            up_pool=up_pool,
            timeouts=timeouts,
            client_sem=client_sem,
            upstream_limits=upstream_limits
        ),
        host="127.0.0.1",
        port=8888,
        backlog=8192
    )

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
