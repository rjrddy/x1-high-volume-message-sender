"""Redis Streams sender with dedupe, consumer recovery and receipt reconciliation."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from collections.abc import Iterator
from pathlib import Path
import redis

ENQUEUE = """if redis.call('HEXISTS','payloads',ARGV[1])==1 then
if redis.call('HGET','payloads',ARGV[1])~=ARGV[2] then return -1 end
return 0 end
redis.call('HSET','payloads',ARGV[1],ARGV[2]);redis.call('HSET','enqueued',ARGV[1],ARGV[3]);
redis.call('XADD','queue','*','id',ARGV[1]);return 1"""
DELIVER = """if redis.call('HEXISTS','receipts',ARGV[1])==1 then return 0 end
redis.call('HSET','receipts',ARGV[1],ARGV[2]);redis.call('HINCRBY','sends',ARGV[1],1);return 1"""
RATE = """local now=tonumber(ARGV[1]); local next=math.max(now,tonumber(redis.call('GET','next-send') or now));
redis.call('SET','next-send',next+tonumber(ARGV[2]));return tostring(next-now)"""


def enqueue(client: redis.Redis, job_id: str, payload: str) -> bool:
    result = client.eval(ENQUEUE, 0, job_id, payload, time.time())
    if result == -1:
        raise ValueError("idempotency conflict")
    return result == 1


@contextmanager
def server() -> Iterator[tuple[redis.Redis, str]]:
    binary = shutil.which("redis-server") or str(
        Path(".cache/redis-7.2.7/src/redis-server").resolve()
    )
    with tempfile.TemporaryDirectory(prefix="sender-", dir="/tmp") as directory:
        socket = str(Path(directory) / "redis.sock")
        process = subprocess.Popen(
            [binary, "--port", "0", "--unixsocket", socket, "--save", "", "--appendonly", "no"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        client = redis.Redis(unix_socket_path=socket, decode_responses=True)
        try:
            for _ in range(200):
                try:
                    if client.ping():
                        break
                except redis.ConnectionError:
                    time.sleep(0.01)
            else:
                raise RuntimeError("Redis startup failed")
            yield client, socket
        finally:
            client.close()
            process.terminate()
            process.wait(timeout=5)


def worker(socket: str, crash: bool = False) -> None:
    client = redis.Redis(unix_socket_path=socket, decode_responses=True)
    deadline = time.monotonic() + 30
    while client.hlen("confirmed") < client.hlen("payloads"):
        if time.monotonic() > deadline:
            raise TimeoutError("sender deadline exceeded")
        claimed = client.xautoclaim("queue", "senders", "worker", 0, count=1)[1]
        if claimed:
            message_id, fields = claimed[0]
        else:
            batches = client.xreadgroup("senders", "worker", {"queue": ">"}, count=1)
            if not batches:
                time.sleep(0.001)
                continue
            message_id, fields = batches[0][1][0]
        job_id = fields["id"]
        if not client.hexists("receipts", job_id):
            attempt = client.hincrby("attempts", job_id, 1)
            if int(job_id) % 13 == 0 and attempt == 1:
                continue  # injected retryable delivery timeout
            delay = float(client.eval(RATE, 0, time.time(), 0.002))
            if delay > 0:
                time.sleep(delay)
            client.eval(DELIVER, 0, job_id, time.time())
            if crash:
                os._exit(17)  # receipt exists; sender status and stream ACK do not
        client.hset("confirmed", job_id, "sent")
        client.xack("queue", "senders", message_id)
    client.close()


def benchmark() -> dict:
    with server() as (client, socket):
        client.xgroup_create("queue", "senders", id="0", mkstream=True)
        for i in range(200):
            assert enqueue(client, str(i), "synthetic-destination-" + str(i))
            assert not enqueue(client, str(i), "synthetic-destination-" + str(i))
        start = time.perf_counter()
        crash = subprocess.run(
            [sys.executable, __file__, "--worker", socket, "--crash"], check=False, timeout=35
        )
        assert crash.returncode == 17
        pending_before = client.xpending("queue", "senders")["pending"]
        subprocess.run([sys.executable, __file__, "--worker", socket], check=True, timeout=35)
        elapsed = time.perf_counter() - start
        receipts = client.hgetall("receipts")
        enqueued = client.hgetall("enqueued")
        sends = client.hgetall("sends")
        latency = sorted((float(at) - float(enqueued[job])) * 1000 for job, at in receipts.items())
        duplicate = sum(max(0, int(n) - 1) for n in sends.values())
        assert len(receipts) == 200 and duplicate == 0
        report = {
            "receipts": receipts,
            "send_counts": sends,
            "pending_before_reconciliation": pending_before,
            "pending_after": client.xpending("queue", "senders")["pending"],
            "latencies_ms": latency,
        }
        Path("evidence/reconciliation.json").write_text(json.dumps(report, indent=2))
        return {
            "messages": len(receipts),
            "throughput_per_second": len(receipts) / elapsed,
            "duplicate_send_rate": duplicate / len(receipts),
            "p99_enqueue_to_send_ms": latency[197],
            "worker_crashes": 1,
            "pending_after_reconciliation": report["pending_after"],
            "rate_limit_per_second": 500,
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker")
    parser.add_argument("--crash", action="store_true")
    args = parser.parse_args()
    if args.worker:
        worker(args.worker, args.crash)
    else:
        print(json.dumps(benchmark(), indent=2))
