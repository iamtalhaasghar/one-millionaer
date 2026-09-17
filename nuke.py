#!/usr/bin/env python3

import argparse
import multiprocessing
import os
import sys
import time

import redis

PREFIX_DEFAULT = "/mnt/data/o365/tweets"
SOURCE_KEY = "mnt:data:o365:data:files"

COUNTER_DONE = "nuke:done_count"
COUNTER_BYTES = "nuke:done_bytes"
COUNTER_ERRORS = "nuke:errors"
COUNTER_START = "nuke:started"
ERROR_SET = "nuke:error_paths"

BATCH = 200
PAGE = 10000
EMPTY_POLL = 0.2
EMPTY_STRIKES = 3


def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Delete files listed in a Redis list using multiprocess workers.",
    )
    p.add_argument("--prefix", default=PREFIX_DEFAULT, help="directory prefix for the relative paths")
    p.add_argument("--src", default=SOURCE_KEY, help="source Redis list key")
    p.add_argument("--queue", default=None, help="work queue key (default: <src>:nuke)")
    p.add_argument("--workers", type=int, default=min(os.cpu_count() or 1, 16))
    p.add_argument("--fresh", action="store_true", help="drop existing queue/counters and re-copy from source")
    p.add_argument("--yes", action="store_true", help="skip confirmation prompt")
    p.add_argument("--keep-errors", action="store_true", help="store failed paths in a Redis set")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=0)
    return p.parse_args(argv)


def make_client(args):
    return redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=True)


def copy_list(r, src, dst):
    try:
        r.copy(src, dst, replace=True)
        return
    except redis.ResponseError:
        pass
    offset = 0
    while True:
        items = r.lrange(src, offset, offset + PAGE - 1)
        if not items:
            break
        r.rpush(dst, *items)
        if len(items) < PAGE:
            break
        offset += len(items)


def prepare_queue(args):
    r = make_client(args)
    queue = args.queue or (args.src + ":nuke")
    if queue == args.src:
        sys.exit(f"refusing: queue key must differ from source key ({queue})")
    if args.fresh:
        r.delete(queue)
        r.delete(COUNTER_DONE, COUNTER_BYTES, COUNTER_ERRORS, COUNTER_START, ERROR_SET)
    if not r.exists(queue):
        if not r.exists(args.src):
            sys.exit(f"source key not found: {args.src}")
        copy_list(r, args.src, queue)
    return r, queue


def safe_path(prefix, rel):
    if not rel or rel.startswith("/") or rel.startswith("\\"):
        return None
    if ".." in rel.replace("\\", "/").split("/"):
        return None
    real_prefix = os.path.realpath(prefix)
    real = os.path.realpath(os.path.join(prefix, rel))
    if real != real_prefix and not real.startswith(real_prefix + os.sep):
        return None
    return real


def pop_batch(r, queue, batch):
    first = r.blpop([queue], timeout=1)
    if first is None:
        return []
    items = [first[1]]
    while len(items) < batch:
        item = r.rpop(queue)
        if item is None:
            break
        items.append(item)
    return items


def worker(host, port, db, queue, prefix, keep_errors, worker_id):
    r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
    empty = 0
    while True:
        popped = pop_batch(r, queue, BATCH)
        if not popped:
            empty += 1
            if empty >= EMPTY_STRIKES and r.llen(queue) == 0:
                break
            time.sleep(EMPTY_POLL)
            continue
        empty = 0
        done = 0
        bytes_done = 0
        errors = 0
        failed = []
        for rel in popped:
            full = safe_path(prefix, rel)
            if full is None:
                errors += 1
                if keep_errors:
                    failed.append(rel)
                continue
            try:
                st = os.stat(full)
                os.remove(full)
                done += 1
                bytes_done += st.st_size
            except FileNotFoundError:
                done += 1
            except OSError:
                errors += 1
                if keep_errors:
                    failed.append(rel)
        pipe = r.pipeline()
        if done:
            pipe.incrby(COUNTER_DONE, done)
            pipe.incrby(COUNTER_BYTES, bytes_done)
        if errors:
            pipe.incrby(COUNTER_ERRORS, errors)
        if failed:
            pipe.sadd(ERROR_SET, *failed)
        pipe.execute()


def fmt_duration(secs):
    if secs is None or secs < 0:
        return "?"
    secs = int(secs)
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}"


def monitor(r, queue, total_items):
    start = float(r.get(COUNTER_START) or time.time())
    done = int(r.get(COUNTER_DONE) or 0)
    errs = int(r.get(COUNTER_ERRORS) or 0)
    bdone = int(r.get(COUNTER_BYTES) or 0)
    last_t = time.time()
    last_processed = done + errs
    last_bdone = bdone
    files_s = 0.0
    bytes_s = 0.0
    while True:
        time.sleep(0.5)
        now = time.time()
        done = int(r.get(COUNTER_DONE) or 0)
        errs = int(r.get(COUNTER_ERRORS) or 0)
        bdone = int(r.get(COUNTER_BYTES) or 0)
        processed = done + errs
        dt = now - last_t
        if dt > 0:
            files_s = 0.7 * files_s + 0.3 * ((processed - last_processed) / dt)
            bytes_s = 0.7 * bytes_s + 0.3 * ((bdone - last_bdone) / dt)
        last_t = now
        last_processed = processed
        last_bdone = bdone
        remaining = max(total_items - processed, 0)
        pct = 100.0 * processed / total_items if total_items else 100.0
        eta = remaining / files_s if files_s > 0 else None
        elapsed = now - start
        line = (
            f"[{pct:6.2f}%] {processed:,}/{total_items:,} files | "
            f"{files_s:,.0f} files/s | {bytes_s / 1e6:,.2f} MB/s | "
            f"ETA {fmt_duration(eta)} | {fmt_duration(elapsed)} elapsed | "
            f"{errs:,} errors"
        )
        sys.stdout.write("\r" + line)
        sys.stdout.flush()
        if remaining <= 0:
            break
    sys.stdout.write("\n")


def main(argv=None):
    args = parse_args(argv)
    if not os.path.isdir(args.prefix):
        sys.exit(f"prefix is not a directory: {args.prefix}")
    r, queue = prepare_queue(args)
    done0 = int(r.get(COUNTER_DONE) or 0)
    err0 = int(r.get(COUNTER_ERRORS) or 0)
    qlen = r.llen(queue)
    total_items = qlen + done0 + err0
    if total_items == 0:
        print("nothing to delete")
        return
    if not args.yes:
        print(f"Will delete {total_items:,} files under {args.prefix}")
        print(f"Work queue: {queue} ({qlen:,} pending)")
        if input("Type YES to continue: ").strip() != "YES":
            print("aborted")
            return
    r.set(COUNTER_START, time.time(), nx=True)
    ctx = multiprocessing.get_context("spawn")
    procs = [
        ctx.Process(
            target=worker,
            args=(args.host, args.port, args.db, queue, args.prefix, args.keep_errors, i),
            daemon=True,
        )
        for i in range(args.workers)
    ]
    for p in procs:
        p.start()
    try:
        monitor(r, queue, total_items)
    except KeyboardInterrupt:
        print("\ninterrupt received, waiting for workers to drain...")
    for p in procs:
        p.join()
    done = int(r.get(COUNTER_DONE) or 0)
    errs = int(r.get(COUNTER_ERRORS) or 0)
    print(f"done: {done:,} files removed, {errs:,} errors, queue empty: {r.llen(queue) == 0}")


if __name__ == "__main__":
    main()
