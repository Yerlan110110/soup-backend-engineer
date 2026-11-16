# Technical references consulted

SQLite's documentation describes serialized writes, WAL readers and snapshot
isolation. The code uses `BEGIN IMMEDIATE` so candidate selection happens after
acquiring the writer lock; it does not perform a read-then-write upgrade from an
old snapshot. [SQLite isolation](https://www.sqlite.org/isolation.html),
[transaction control](https://www.sqlite.org/lang_transaction.html).

WAL requires processes to share the same host and is unsuitable for this design
on a network filesystem. [SQLite WAL](https://www.sqlite.org/wal.html).

FastAPI runs synchronous path operations in its thread pool, allowing the synchronous
SQLite adapter to keep explicit transaction boundaries without blocking the event
loop. [FastAPI concurrency](https://fastapi.tiangolo.com/async/).

Compose's replica configuration and uv's container installation pattern informed
the supplied Docker files; reading these references is not an executed container test.
[Compose deploy specification](https://docs.docker.com/reference/compose-file/deploy/),
[uv Docker integration](https://docs.astral.sh/uv/guides/integration/docker/).
