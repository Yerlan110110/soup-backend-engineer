import socket

import httpx2


def client(base_url: str) -> httpx2.AsyncClient:
    limits = httpx2.Limits(max_connections=100, max_keepalive_connections=40, keepalive_expiry=30)
    transport = httpx2.AsyncHTTPTransport(
        http2=True,
        retries=3,
        limits=limits,
        socket_options=[(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
    )
    return httpx2.AsyncClient(
        base_url=base_url,
        transport=transport,
        timeout=httpx2.Timeout(connect=5, read=15, write=10, pool=10),
        follow_redirects=True,
        trust_env=False,
    )
