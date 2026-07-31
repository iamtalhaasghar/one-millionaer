#!/usr/bin/env python3

import asyncio
import json
import logging
import os

import aiomysql
import redis.asyncio as redis
import time

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

TOTAL_TWEETS=2_683_941

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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


async def producer():

    start = time.monotonic()

    r = redis.Redis(**REDIS)

    logging.info("Connecting to MySQL")

    conn = await aiomysql.connect(
        **MYSQL,
        cursorclass=aiomysql.SSCursor,
    )

    async with conn.cursor() as cur:

        logging.info("Executing query")

        await cur.execute(
            """
            SELECT concat(download_path,'/', name), created_at
            FROM files
            WHERE download_path IS NOT NULL
            """
        )

        count = 0
        skipped = 0

        async for row in cur:
            try:
                download_path, created_at = row
                count += 1

                if not os.path.exists(download_path):
                    skipped += 1
                    continue

                payload = {
                    "download_path": download_path,
                    "created_at": created_at.isoformat(),
                }

                await r.rpush(
                    QUEUE_NAME,
                    json.dumps(payload),
                )


                if count % 10000 == 0:
                    qsize = await r.llen(QUEUE_NAME)

                    elapsed = time.monotonic() - start
                    rate = count / elapsed if elapsed else 0
                    remaining = TOTAL_TWEETS - count
                    eta = remaining / rate if rate else 0
                    eta = time.strftime("%H:%M:%S", time.gmtime(eta))

                    logging.info(
                        "Produced=%d skipped=%d queue=%d rate=%d eta=%s",
                        count,
                        skipped,
                        qsize,
                        rate,
                        eta,
                    )
            except Exception as e:
                logging.exception(e)

    await r.close()
    conn.close()

    logging.info(
        "Producer finished. Total=%d skipped=%d",
        count,
        skipped,
    )


if __name__ == "__main__":
    asyncio.run(producer())
