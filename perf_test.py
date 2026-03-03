#!/usr/bin/env python3

import json
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://127.0.0.1:8080"


def request(method, path, data=None):
    url = BASE_URL + path
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("Content-Type", "application/json; charset=utf-8")
    else:
        req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def post_record(record_id, data):
    status, body = request("POST", "/records", {"id": record_id, "data": data})
    return status == 201


def get_record(record_id):
    status, body = request("GET", f"/records/{record_id}")
    return status == 200 and body.get("id") == record_id


def get_all_records():
    status, body = request("GET", "/records")
    return status == 200 and "records" in body


def run_sequential(writes=100, reads_per_write=2):
    start = time.perf_counter()
    for i in range(writes):
        post_record(f"seq-{i}", {"value": i, "text": f"record {i}"})
    reads = 0
    for i in range(writes):
        for _ in range(reads_per_write):
            if get_record(f"seq-{i}"):
                reads += 1
    elapsed = time.perf_counter() - start
    total_ops = writes + writes * reads_per_write
    return total_ops, elapsed, total_ops / elapsed if elapsed else 0


def run_concurrent(num_workers=10, ops_per_worker=50):
    def worker(wid):
        local_ok = 0
        for i in range(ops_per_worker):
            rid = f"concurrent-w{wid}-{i}"
            if post_record(rid, {"w": wid, "i": i}):
                local_ok += 1
            if get_record(rid):
                local_ok += 1
        return local_ok

    start = time.perf_counter()
    total_ok = 0
    with ThreadPoolExecutor(max_workers=num_workers) as ex:
        futures = [ex.submit(worker, w) for w in range(num_workers)]
        for f in as_completed(futures):
            total_ok += f.result()
    elapsed = time.perf_counter() - start
    total_ops = num_workers * ops_per_worker * 2
    return total_ops, elapsed, total_ops / elapsed if elapsed else 0


def run_bulk_get(num_records=500, repeat=5):
    for i in range(num_records):
        post_record(f"bulk-{i}", {"n": i})
    start = time.perf_counter()
    for _ in range(repeat):
        get_all_records()
    elapsed_all = time.perf_counter() - start
    ids = ",".join(f"bulk-{i}" for i in range(0, num_records, 10))
    start = time.perf_counter()
    for _ in range(repeat):
        status, body = request("GET", f"/records?ids={ids}")
    elapsed_ids = time.perf_counter() - start
    return (
        repeat / elapsed_all if elapsed_all else 0,
        repeat / elapsed_ids if elapsed_ids else 0,
    )


def main():
    import sys
    if len(sys.argv) > 1:
        global BASE_URL
        BASE_URL = sys.argv[1].rstrip("/")

    print(f"Base URL: {BASE_URL}")
    try:
        _, body = request("GET", "/records")
        print("Server OK\n")
    except Exception as e:
        print(f"Error: {e}. Start server: python server.py")
        return 1

    print("1) Sequential (100 writes + 2 reads each)")
    total, elapsed, rps = run_sequential(100, 2)
    print(f"   Ops: {total}, time: {elapsed:.2f} s, ~{rps:.0f} ops/s\n")

    print("2) Concurrent (10 workers x 50 write+read pairs)")
    total, elapsed, rps = run_concurrent(10, 50)
    print(f"   Ops: {total}, time: {elapsed:.2f} s, ~{rps:.0f} ops/s\n")

    print("3) Bulk get (200 records, then GET /records and GET ?ids=...)")
    rps_all, rps_ids = run_bulk_get(200, 5)
    print(f"   GET /records:      ~{rps_all:.1f} req/s")
    print(f"   GET /records?ids=: ~{rps_ids:.1f} req/s\n")

    print("Done.")
    return 0


if __name__ == "__main__":
    exit(main())
