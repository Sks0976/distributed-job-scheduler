#!/usr/bin/env python3
"""
Submits N jobs through the batch API and measures end-to-end throughput
(time from first submission until the queue drains). Standard library only.

    python3 scripts/load_test.py --url http://localhost:8081 --jobs 10000 --work-ms 20

Numbers depend on your machine, node count and WORKER_THREADS; quote what you measure.
"""
import argparse
import json
import time
import urllib.request


def call(url, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8081")
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--work-ms", type=int, default=20, help="simulated work per job")
    args = parser.parse_args()

    before = call(f"{args.url}/api/jobs/stats")
    baseline_done = before["SUCCEEDED"] + before["FAILED"]

    start = time.time()
    submitted = 0
    while submitted < args.jobs:
        size = min(args.batch, args.jobs - submitted)
        jobs = [{"type": "sleep", "payload": {"millis": args.work_ms}} for _ in range(size)]
        submitted += call(f"{args.url}/api/jobs/batch", "POST", {"jobs": jobs})["created"]
    print(f"Submitted {submitted} jobs in {time.time() - start:.1f}s")

    while True:
        stats = call(f"{args.url}/api/jobs/stats")
        done = stats["SUCCEEDED"] + stats["FAILED"] - baseline_done
        print(f"\r  done {done}/{submitted}  pending {stats['PENDING']}  running {stats['RUNNING']}   ", end="", flush=True)
        if done >= submitted:
            break
        time.sleep(0.5)

    elapsed = time.time() - start
    print(f"\nProcessed {submitted} jobs in {elapsed:.1f}s -> {submitted / elapsed * 60:,.0f} jobs/min")


if __name__ == "__main__":
    main()
