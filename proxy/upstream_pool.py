import asyncio

class UpstreamConnPool:
    def __init__(self, backends, max_idle=100):
        self._pools = {
            b: asyncio.LifoQueue(max_idle)
            for b in backends
        }

    async def acquire(self, backend):
        pool = self._pools[backend]

        try:
            reader, writer = pool.get_nowait()

            if reader.at_eof() or writer.is_closing():
                writer.close()
                return None

            return reader, writer

        except asyncio.QueueEmpty:
            return None

    async def release(self, backend, conn):
        pool = self._pools[backend]
        reader, writer = conn

        if writer.is_closing():
            return

        if pool.full():
            writer.close()
        else:
            await pool.put(conn)