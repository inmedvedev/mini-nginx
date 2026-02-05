import asyncio
import logging
import socket
import time


BUF = 256 * 1024
logger = logging.getLogger("proxy")


class Deadline:
    def __init__(self, timeout: float):
        self.end = time.monotonic() + timeout

    def left(self) -> float:
        return max(0.0, self.end - time.monotonic())


def get_timeout(deadline: Deadline, op_timeout: float | None) -> float:
    left = deadline.left()
    if op_timeout is None:
        return left
    return min(left, op_timeout)


def set_nodelay(writer: asyncio.StreamWriter):
    sock = writer.get_extra_info("socket")
    if sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


async def read_headers(reader: asyncio.StreamReader):
    data = await reader.readuntil(b"\r\n\r\n")
    lines = data.split(b"\r\n")
    method, path, version = lines[0].split()
    headers = {}

    for line in lines[1:]:
        if not line:
            break
        k, v = line.split(b":", 1)
        headers[k.lower()] = v.strip().lower()

    return method, path, version, headers, data


async def stream_fixed(reader, writer, size, deadline, read_timeout, write_timeout):
    left = size

    while left > 0:
        chunk = await asyncio.wait_for(
            reader.read(min(BUF, left)),
            get_timeout(deadline, read_timeout)
        )

        if not chunk:
            break

        writer.write(chunk)
        await asyncio.wait_for(
            writer.drain(),
            get_timeout(deadline, write_timeout)
        )
        left -= len(chunk)


async def stream_chunked(reader, writer, deadline, read_timeout, write_timeout):
    while True:
        line = await asyncio.wait_for(
            reader.readline(),
            get_timeout(deadline, read_timeout)
        )

        writer.write(line)
        await asyncio.wait_for(
            writer.drain(),
            get_timeout(deadline, write_timeout)
        )

        size = int(line.strip(), 16)

        if size == 0:
            trailer = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                get_timeout(deadline, read_timeout)
            )
            writer.write(trailer)
            await asyncio.wait_for(
                writer.drain(),
                get_timeout(deadline, write_timeout)
            )
            break

        data = await asyncio.wait_for(
            reader.readexactly(size + 2),
            get_timeout(deadline, read_timeout)
        )
        writer.write(data)
        await asyncio.wait_for(
            writer.drain(),
            get_timeout(deadline, write_timeout)
        )


async def pipe(reader, writer, deadline, read_timeout, write_timeout):
    try:
        while not reader.at_eof():
            data = await asyncio.wait_for(
                reader.read(BUF),
                get_timeout(deadline, read_timeout)
            )

            if not data:
                break

            writer.write(data)
            await asyncio.wait_for(
                writer.drain(),
                get_timeout(deadline, write_timeout)
            )

    except Exception:
        pass

    finally:
        try:
            writer.write_eof()
        except Exception:
            pass

        writer.close()

async def handle_client(
    reader,
    writer,
    pool,
    up_pool,
    timeouts,
    client_sem,
    upstream_limits,
):
    async with client_sem:

        while True:
            deadline = Deadline(timeouts.total)
            path_str = ""
            method_str = ""
            status_code: int | None = None

            try:
                method, path, ver, headers, raw = await asyncio.wait_for(
                    read_headers(reader),
                    get_timeout(deadline, timeouts.read)
                )

                method_str = method.decode(errors="replace")
                path_str = path.decode(errors="replace")

            except Exception:
                break

            backend = pool.next()
            host, port = backend
            limit = upstream_limits[backend]

            try:
                async with limit:

                    conn = await up_pool.acquire(backend)

                    if conn:
                        up_reader, up_writer = conn

                    else:
                        up_reader, up_writer = await asyncio.wait_for(
                            asyncio.open_connection(host, port),
                            get_timeout(deadline, timeouts.connect)
                        )
                        set_nodelay(up_writer)

            except Exception:
                await send_502(writer)
                break

            try:
                up_writer.write(raw)
                await asyncio.wait_for(
                    up_writer.drain(),
                    get_timeout(deadline, timeouts.write)
                )

                if b"content-length" in headers:
                    await stream_fixed(
                        reader,
                        up_writer,
                        int(headers[b"content-length"]),
                        deadline,
                        timeouts.read,
                        timeouts.write
                    )

                elif headers.get(b"transfer-encoding") == b"chunked":
                    await stream_chunked(
                        reader,
                        up_writer,
                        deadline,
                        timeouts.read,
                        timeouts.write
                    )

                resp_hdr = await asyncio.wait_for(
                    up_reader.readuntil(b"\r\n\r\n"),
                    get_timeout(deadline, timeouts.read)
                )

                writer.write(resp_hdr)
                await asyncio.wait_for(
                    writer.drain(),
                    get_timeout(deadline, timeouts.write)
                )

                resp_lines = resp_hdr.split(b"\r\n")
                resp_headers = {}

                if resp_lines:
                    try:
                        status_code = int(resp_lines[0].split()[1])
                    except Exception:
                        status_code = None

                for line in resp_lines[1:]:
                    if not line:
                        break

                    k, v = line.split(b":", 1)
                    resp_headers[k.lower()] = v.strip().lower()

                if b"content-length" in resp_headers:
                    await stream_fixed(
                        up_reader,
                        writer,
                        int(resp_headers[b"content-length"]),
                        deadline,
                        timeouts.read,
                        timeouts.write
                    )

                elif resp_headers.get(b"transfer-encoding") == b"chunked":
                    await stream_chunked(
                        up_reader,
                        writer,
                        deadline,
                        timeouts.read,
                        timeouts.write
                    )

                else:
                    await pipe(
                        up_reader,
                        writer,
                        deadline,
                        timeouts.read,
                        timeouts.write
                    )

            except asyncio.TimeoutError:
                logger.error(
                    "method=%s path=%s status=504",
                    method_str,
                    path_str
                )
                await send_504(writer)
                break

            except Exception as e:
                logger.error(
                    "method=%s path=%s status=502 error=%s",
                    method_str,
                    path_str,
                    type(e).__name__
                )
                await send_502(writer)
                break

            finally:
                await up_pool.release(
                    backend,
                    (up_reader, up_writer)
                )

            # logger.info(
            #     "method=%s path=%s status=%s",
            #     method_str,
            #     path_str,
            #     status_code if status_code is not None else "-"
            # )

            if (
                headers.get(b"connection") == b"close" 
                or resp_headers.get(b"connection") == b"close"
            ):
                break

        writer.close()


async def send_502(writer):
    body = b"Bad Gateway"

    resp = (
        b"HTTP/1.1 502 Bad Gateway\r\n"
        b"Connection: close\r\n"
        b"Content-Length: "
        + str(len(body)).encode()
        + b"\r\n\r\n"
        + body
    )

    writer.write(resp)
    await writer.drain()
    writer.close()


async def send_504(writer):
    body = b"Gateway Timeout"

    resp = (
        b"HTTP/1.1 504 Gateway Timeout\r\n"
        b"Connection: close\r\n"
        b"Content-Length: "
        + str(len(body)).encode()
        + b"\r\n\r\n"
        + body
    )

    writer.write(resp)
    await writer.drain()
    writer.close()