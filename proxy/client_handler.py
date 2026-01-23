import asyncio
import socket
import time


BUF = 256 * 1024


class Deadline:
    def __init__(self, timeout: float):
        self.end = time.monotonic() + timeout

    def left(self) -> float:
        return max(0.0, self.end - time.monotonic())


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


async def stream_fixed(reader, writer, size, deadline):
    left = size

    while left > 0:
        chunk = await asyncio.wait_for(
            reader.read(min(BUF, left)),
            deadline.left()
        )

        if not chunk:
            break

        writer.write(chunk)
        await writer.drain()
        left -= len(chunk)


async def stream_chunked(reader, writer, deadline):
    while True:
        line = await asyncio.wait_for(
            reader.readline(),
            deadline.left()
        )

        writer.write(line)
        await writer.drain()

        size = int(line.strip(), 16)

        if size == 0:
            trailer = await reader.readuntil(b"\r\n\r\n")
            writer.write(trailer)
            await writer.drain()
            break

        data = await reader.readexactly(size + 2)
        writer.write(data)
        await writer.drain()


async def pipe(reader, writer, deadline):
    try:
        while not reader.at_eof():
            data = await asyncio.wait_for(
                reader.read(BUF),
                deadline.left()
            )

            if not data:
                break

            writer.write(data)
            await writer.drain()

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

            try:
                method, path, ver, headers, raw = await asyncio.wait_for(
                    read_headers(reader),
                    deadline.left()
                )

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
                            deadline.left()
                        )
                        set_nodelay(up_writer)

            except Exception:
                await send_502(writer)
                break

            try:
                up_writer.write(raw)
                await up_writer.drain()

                if b"content-length" in headers:
                    await stream_fixed(
                        reader,
                        up_writer,
                        int(headers[b"content-length"]),
                        deadline
                    )

                elif headers.get(b"transfer-encoding") == b"chunked":
                    await stream_chunked(
                        reader,
                        up_writer,
                        deadline
                    )

                resp_hdr = await asyncio.wait_for(
                    up_reader.readuntil(b"\r\n\r\n"),
                    deadline.left()
                )

                writer.write(resp_hdr)
                await writer.drain()

                resp_lines = resp_hdr.split(b"\r\n")
                resp_headers = {}

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
                        deadline
                    )

                elif resp_headers.get(b"transfer-encoding") == b"chunked":
                    await stream_chunked(
                        up_reader,
                        writer,
                        deadline
                    )

                else:
                    await pipe(
                        up_reader,
                        writer,
                        deadline
                    )

            except asyncio.TimeoutError:
                await send_504(writer)
                break

            except Exception as e:
                await send_502(writer)
                break

            finally:
                await up_pool.release(
                    backend,
                    (up_reader, up_writer)
                )

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