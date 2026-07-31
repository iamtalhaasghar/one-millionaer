#!/usr/bin/env python3

import argparse
import asyncio
import json
import logging
import os
import shutil
import time

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


DST_DIR = "/mnt/data/o365/sorted_tweets"


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

    os.makedirs(
        os.path.dirname(dst),
        exist_ok=True,
    )

    if os.path.exists(dst):
        return "exists"

    os.rename(
        src,
        dst,
    )

    return "copied"


async def worker(worker_id, r):

    processed = 0
    copied = 0
    missing = 0

    logging.info(
        "Worker %d started",
        worker_id,
    )

    while True:

        item = await r.blpop(
            QUEUE_NAME,
            timeout=0,
        )

        payload = json.loads(
            item[1]
        )

        processed += 1

        try:

            result = await asyncio.to_thread(
                copy_file,
                payload,
            )

            if result == "copied":
                copied += 1

        except FileNotFoundError as e:
            logging.warning(e)
            missing += 1

        except Exception:

            logging.exception(
                "Worker %d failed %s",
                worker_id,
                payload["download_path"],
            )


        if processed % 100 == 0:

            logging.info(
                "Worker=%d processed=%d copied=%d missing=%d",
                worker_id,
                processed,
                copied,
                missing,
            )


async def main(workers):

    r = redis.Redis(**REDIS)

    tasks = []

    for i in range(workers):

        tasks.append(
            asyncio.create_task(
                worker(
                    i + 1,
                    r,
                )
            )
        )

    await asyncio.gather(
        *tasks
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--workers",
        type=int,
        default=16,
    )

    args = parser.parse_args()

    asyncio.run(
        main(
            args.workers
        )
    )
