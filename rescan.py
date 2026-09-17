#!/usr/bin/env python3

import argparse
import asyncio
import json
import logging
import os
import time

import aiomysql
import redis.asyncio as redis


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

MYSQL = {
    "host": "127.0.0.1",
    "user": "admin",
    "password": "admin",
    "db": "tweets",
    "charset": "utf8mb4",
}

REDIS = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0,
}

QUEUE_NAME = "tweet_copy_queue"

SCAN_DIR = "/mnt/data/o365/tweets"

LOOKUP_CHUNK = 1000
PUSH_CHUNK = 1000


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


async def lookup_created_at(cur, names, table):

    found = {}

    for i in range(0, len(names), LOOKUP_CHUNK):

        chunk = names[i : i + LOOKUP_CHUNK]

        placeholders = ",".join(["%s"] * len(chunk))

        await cur.execute(
            f"SELECT name, created_at FROM {table} WHERE name IN ({placeholders})",
            chunk,
        )

        for row in await cur.fetchall():
            found[row["name"]] = row["created_at"]

    return found


def read_json_created_at(name):
    exit()
    path = os.path.join(SCAN_DIR, name)

    try:
        with open(path) as f:
            obj = json.load(f)

        data = obj.get("data") or obj

        return data.get("created_at")

    except Exception:
        return None


async def main(dry_run):

    start = time.monotonic()

    names = [
        entry.name
        for entry in os.scandir(SCAN_DIR)
        if entry.is_file() and entry.name.endswith(".json")
    ]

    logging.info(
        "Found %d files in %s",
        len(names),
        SCAN_DIR,
    )

    conn = await aiomysql.connect(
        **MYSQL,
        cursorclass=aiomysql.DictCursor,
    )

    async with conn.cursor() as cur:

        tweets_map = await lookup_created_at(cur, names, "tweets")
        files_map = await lookup_created_at(cur, names, "files")

    conn.close()

    payloads = []
    not_found = 0
    no_date = 0

    for name in names:

        created_at = tweets_map.get(name)

        if created_at is None:
            created_at = files_map.get(name)

        #if created_at is None:
        #    created_at = read_json_created_at(name)

        if created_at is None:
            not_found += 1
            continue

        if hasattr(created_at, "isoformat"):
            created_at = created_at.isoformat()

        payloads.append(
            {
                "download_path": os.path.join(SCAN_DIR, name),
                "created_at": created_at,
            }
        )

    if dry_run:

        logging.info(
            "DRY RUN: %d payloads, %d not_found, %d no_date",
            len(payloads),
            not_found,
            no_date,
        )

        for payload in payloads[:10]:
            logging.info(
                "%s -> %s",
                payload["download_path"],
                payload["created_at"],
            )

        return

    r = redis.Redis(**REDIS)

    pipe = r.pipeline()

    for i, payload in enumerate(payloads, 1):

        pipe.rpush(
            QUEUE_NAME,
            json.dumps(payload),
        )

        if i % PUSH_CHUNK == 0:
            await pipe.execute()

            elapsed = time.monotonic() - start
            rate = i / elapsed if elapsed else 0
            remaining = len(payloads) - i
            eta = remaining / rate if rate else 0
            eta = time.strftime("%H:%M:%S", time.gmtime(eta))

            logging.info(
                "Pushed=%d rate=%d eta=%s",
                i,
                rate,
                eta,
            )

    if len(payloads) % PUSH_CHUNK:
        await pipe.execute()

    await r.close()

    elapsed = time.monotonic() - start

    logging.info(
        "Done. Pushed=%d not_found=%d no_date=%d in %.1fs",
        len(payloads),
        not_found,
        no_date,
        elapsed,
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dry-run",
        action="store_true",
    )

    args = parser.parse_args()

    asyncio.run(
        main(
            args.dry_run
        )
    )
