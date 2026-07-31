#!/usr/bin/env python3

import argparse
import asyncio
import itertools
import json
import logging
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor

import redis.asyncio as redis


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

REDIS = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0,
}

QUEUE_NAME = "tweet_copy_queue"

TOTAL_TWEETS = 2_683_941

BATCH = 16


DST_DIR = "/mnt/data/o365/sorted_tweets"

_created_dirs = set()


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def copy_file(data):

    src = data["download_path"]

    created_at = data["created_at"]

    dt = created_at[:10].split("-")

    yyyy = dt[0]
    mm = dt[1]
    dd = dt[2]

    dst_dir = os.path.join(
        DST_DIR,
        yyyy,
        mm,
        dd,
    )
    filename = os.path.basename(src)
    dst = os.path.join(
        dst_dir,
        filename,
    )

    if dst_dir not in _created_dirs:
        os.makedirs(
            dst_dir,
            exist_ok=True,
        )
        _created_dirs.add(dst_dir)

    #if os.path.exists(dst):
    #    return "exists"


    os.rename(
        src,
        dst,
    )

    return "copied"


async def worker(worker_id, r, processed, start, executor):

    copied = 0
    missing = 0

    logging.info(
        "Worker %d started",
        worker_id,
    )

    while True:

        pipe = r.pipeline()

        for _ in range(BATCH):
            pipe.lpop(QUEUE_NAME)

        items = await pipe.execute()

        items = [
            item
            for item in items
            if item is not None
        ]

        if not items:
            await r.blpop(
                QUEUE_NAME,
                timeout=0,
            )
            continue

        entries = []

        for item in items:
            payload = json.loads(item)
            processed_count = next(processed)
            entries.append(
                (payload, processed_count)
            )

        loop = asyncio.get_running_loop()

        tasks = [
            loop.run_in_executor(
                executor,
                copy_file,
                payload,
            )
            for payload, _ in entries
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        for (payload, processed_count), result in zip(entries, results):

            if isinstance(result, FileNotFoundError):
                logging.warning(result)
                missing += 1

            elif isinstance(result, Exception):
                logging.exception(
                    "Worker %d failed %s",
                    worker_id,
                    payload["download_path"],
                )

            elif result == "copied":
                copied += 1

            if processed_count % 500 == 0:

                elapsed = time.monotonic() - start
                rate = processed_count / elapsed if elapsed else 0
                remaining = TOTAL_TWEETS - processed_count
                eta = remaining / rate if rate else 0
                eta = time.strftime("%H:%M:%S", time.gmtime(eta))

                logging.info(
                    "Worker=%d processed=%d copied=%d missing=%d rate=%d eta=%s",
                    worker_id,
                    processed_count,
                    copied,
                    missing,
                    rate,
                    eta,
                )


async def main(workers):

    r = redis.Redis(**REDIS)

    executor = ThreadPoolExecutor(
        max_workers=workers * BATCH,
    )

    start = time.monotonic()
    processed = itertools.count()

    tasks = []

    for i in range(workers):

        tasks.append(
            asyncio.create_task(
                worker(
                    i + 1,
                    r,
                    processed,
                    start,
                    executor,
                )
            )
        )

    try:
        await asyncio.gather(
            *tasks
        )
    finally:
        executor.shutdown()
        await r.aclose()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--workers",
        type=int,
        default=8,
    )

    args = parser.parse_args()

    asyncio.run(
        main(
            args.workers
        )
    )
