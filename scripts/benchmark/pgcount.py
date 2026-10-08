#!/usr/bin/env python3
"""Count the SQL statements a tableaux backend sends to PostgreSQL. LOCAL USE ONLY.

A TCP proxy for the PostgreSQL wire protocol: the backend connects to the proxy instead of
the database, the proxy forwards all bytes unchanged and parses the client -> server
direction to count messages, e.g. to compare the statements of a cold and a warm request.
Python 3.8+ standard library only.

Never point a production backend at it, and never use it for timing runs: it adds latency.
It needs a plaintext database connection (no TLS between backend and proxy).

USAGE
  python3 scripts/benchmark/pgcount.py serve [--listen-host H] [--listen-port P]
                                            [--control-port C] [--upstream-host H] [--upstream-port P]
  python3 scripts/benchmark/pgcount.py get   [--control-port C] [--top N]
  python3 scripts/benchmark/pgcount.py reset [--control-port C] [--top N]

  serve   Run the proxy in the foreground until Ctrl-C.
            --listen-host    Address the proxy listens on. Default 127.0.0.1.
            --listen-port    Port the backend connects to (set database.port in its config).
                             Default 15432.
            --control-port   Port for get/reset, always bound to 127.0.0.1. Default 15433.
            --upstream-host  Real PostgreSQL host. Default 127.0.0.1.
            --upstream-port  Real PostgreSQL port. Default 5432.
  get     Print the counters since the last reset as JSON.
  reset   Print the counters like get, then set them to zero.
            --control-port   Control port of the running proxy. Default 15433.
            --top N          Number of statement texts listed per table. Default 25.

COUNTERS
  counts:      frontend messages: parse (P), bind (B), execute (E), query (simple query, Q),
               sync (S), terminate (X), plus connections and startup messages.
  statements:  execute + query, i.e. statements run by the database.
  exec_by_sql, parse_by_sql: most frequent statement texts with digits replaced by "#",
               whitespace collapsed, cut to 220 characters. Bind parameters (the values) are
               never looked at. The texts are SQL of the backend, but simple queries may carry
               literals, so treat the output as local diagnostics and do not share it unseen.

COLD VS WARM STATEMENT COUNT OF ONE REQUEST (see README.md)
  1. pgcount.py serve ...; start the backend with database.port = --listen-port.
  2. pgcount.py reset               (drops the statements of the backend start-up)
  3. bench_rows.py ... --cases rows_limit500 --repeat 0    (cold: first request after start)
  4. pgcount.py reset               (prints the cold counts)
  5. bench_rows.py ... --cases rows_limit500 --repeat 0    (warm)
  6. pgcount.py reset               (prints the warm counts)
  Wait a second before steps 4 and 6: the backend may still finish work after responding.
"""

import argparse
import asyncio
import json
import re
import socket
import struct
import sys
from collections import Counter

SSL_REQUEST = 80877103
GSSENC_REQUEST = 80877104
DIGITS = re.compile(r"\d+")
WS = re.compile(r"\s+")

counts = Counter()
exec_by_sql = Counter()
parse_by_sql = Counter()


def norm(sql):
    return WS.sub(" ", DIGITS.sub("#", sql)).strip()[:220]


def cstr(buf, pos):
    end = buf.index(b"\x00", pos)
    return buf[pos:end].decode("utf-8", "replace"), end + 1


async def pipe_plain(reader, writer):
    """server -> client: forward only."""
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        writer.close()


async def pipe_client(reader, writer):
    """client -> server: forward and count complete frontend messages."""
    stmts = {}  # prepared statement name -> sql, per connection
    portals = {}  # portal name -> sql
    buf = b""
    started = False
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            buf += data
            while True:
                if not started:  # startup packet: length + protocol code, no type byte
                    if len(buf) < 8:
                        break
                    ln, code = struct.unpack("!ii", buf[:8])
                    if len(buf) < ln:
                        break
                    buf = buf[ln:]
                    if code in (SSL_REQUEST, GSSENC_REQUEST):  # the real startup packet follows
                        continue
                    started = True
                    counts["startup"] += 1
                    continue
                if len(buf) < 5:
                    break
                kind = buf[0:1]
                (ln,) = struct.unpack("!i", buf[1:5])
                if len(buf) < 1 + ln:
                    break
                body = buf[5:1 + ln]
                buf = buf[1 + ln:]
                if kind == b"P":
                    name, p = cstr(body, 0)
                    sql, _ = cstr(body, p)
                    stmts[name] = sql
                    counts["parse"] += 1
                    parse_by_sql[norm(sql)] += 1
                elif kind == b"B":
                    portal, p = cstr(body, 0)
                    stmt, _ = cstr(body, p)
                    portals[portal] = stmts.get(stmt, "<unknown statement %r>" % stmt)
                    counts["bind"] += 1
                elif kind == b"E":
                    portal, _ = cstr(body, 0)
                    counts["execute"] += 1
                    exec_by_sql[norm(portals.get(portal, "<unknown portal>"))] += 1
                elif kind == b"Q":
                    sql, _ = cstr(body, 0)
                    counts["query"] += 1
                    exec_by_sql["Q: " + norm(sql)] += 1
                elif kind == b"S":
                    counts["sync"] += 1
                elif kind == b"X":
                    counts["terminate"] += 1
            await writer.drain()
    except Exception:
        counts["proxy_error"] += 1
    finally:
        writer.close()


def snapshot(top):
    c = dict(counts)
    return {"statements": c.get("execute", 0) + c.get("query", 0), "counts": c,
            "exec_by_sql": exec_by_sql.most_common(top), "parse_by_sql": parse_by_sql.most_common(top)}


async def serve(args):
    async def handle(client_reader, client_writer):
        try:
            server_reader, server_writer = await asyncio.open_connection(args.upstream_host, args.upstream_port)
        except OSError:
            counts["upstream_connect_error"] += 1
            client_writer.close()
            return
        counts["connections"] += 1
        await asyncio.gather(pipe_client(client_reader, server_writer), pipe_plain(server_reader, client_writer))

    async def control(reader, writer):
        line = (await reader.readline()).decode().split()
        cmd = line[0] if line else "get"
        top = int(line[1]) if len(line) > 1 else 25
        snap = snapshot(top)
        if cmd == "reset":
            counts.clear()
            exec_by_sql.clear()
            parse_by_sql.clear()
        writer.write((json.dumps(snap) + "\n").encode())
        await writer.drain()
        writer.close()

    proxy = await asyncio.start_server(handle, args.listen_host, args.listen_port)
    ctl = await asyncio.start_server(control, "127.0.0.1", args.control_port)
    print("pgcount: %s:%d -> %s:%d, control 127.0.0.1:%d (local use only)" % (
        args.listen_host, args.listen_port, args.upstream_host, args.upstream_port, args.control_port),
        file=sys.stderr, flush=True)
    async with proxy, ctl:
        await asyncio.gather(proxy.serve_forever(), ctl.serve_forever())


def ask(args):
    with socket.create_connection(("127.0.0.1", args.control_port), timeout=10) as s:
        s.sendall(("%s %d\n" % (args.command, args.top)).encode())
        data = b""
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
    print(json.dumps(json.loads(data), indent=1))


def main():
    ap = argparse.ArgumentParser(description="Statement-counting PostgreSQL proxy, local use only. "
                                             "See the module docstring for details.")
    sub = ap.add_subparsers(dest="command", required=True)
    s = sub.add_parser("serve", help="run the proxy")
    s.add_argument("--listen-host", default="127.0.0.1")
    s.add_argument("--listen-port", type=int, default=15432)
    s.add_argument("--control-port", type=int, default=15433)
    s.add_argument("--upstream-host", default="127.0.0.1")
    s.add_argument("--upstream-port", type=int, default=5432)
    for name, text in (("get", "print counters"), ("reset", "print counters, then zero them")):
        c = sub.add_parser(name, help=text)
        c.add_argument("--control-port", type=int, default=15433)
        c.add_argument("--top", type=int, default=25)
    args = ap.parse_args()
    if args.command == "serve":
        try:
            asyncio.run(serve(args))
        except KeyboardInterrupt:
            pass
    else:
        ask(args)


if __name__ == "__main__":
    main()
