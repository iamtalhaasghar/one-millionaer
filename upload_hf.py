#!/usr/bin/env python3

import argparse
import glob
import json
import math
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor

import redis


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

SOURCE_DIR = "/mnt/data/o365/sorted_tweets"
SHARDS_DIR = "/mnt/data/o365/shards"
DATA_DIR = os.path.join(SHARDS_DIR, "data")
MANIFEST_DIR = os.path.join(SHARDS_DIR, "manifest")
FILES_TXT = os.path.join(SHARDS_DIR, "files.txt")
README_MD = os.path.join(SHARDS_DIR, "README.md")
MERGED_MANIFEST = os.path.join(SHARDS_DIR, "manifest.json")

SUMMARY_TXT = "/mnt/data/projects/million-downloader/summary.txt"

FILES_PER_SHARD = 100_000
EXPECTED_FILES = 5_328_686
EXPECTED_DIRS = 770

REDIS = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0,
}

KEY_FILES = "mnt:data:o365:data:files"
KEY_FILES_TMP = KEY_FILES + ":tmp"
KEY_WORK = "mnt:data:o365:work:files"
KEY_WORK_TMP = KEY_WORK + ":tmp"
KEY_SEQ = "mnt:data:o365:shards:seq"
KEY_PACKED = "mnt:data:o365:shards:packed"

KEY_DIRS_FILES = "mnt:data:o365:dirs:files"
KEY_DIRS_WORK = "mnt:data:o365:dirs:work"
KEY_DIRS_DONE = "mnt:data:o365:dirs:done"

DEFAULT_REPO = "iamtalhaasghar/tweets"


def get_redis():
    return redis.Redis(**REDIS)


def fmt_eta(seconds):
    seconds = int(seconds)
    return f"{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}:{seconds % 60:02d}"


def fmt_eta_long(seconds):
    seconds = int(seconds)
    if seconds >= 3600:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    return fmt_eta(seconds)


def built_files_count():
    total = 0
    if os.path.isdir(MANIFEST_DIR):
        for name in os.listdir(MANIFEST_DIR):
            if name.endswith(".json"):
                with open(os.path.join(MANIFEST_DIR, name)) as f:
                    entry = json.load(f)
                if entry.get("built"):
                    total += entry.get("files", 0)
    return total


def shard_manifest_path(idx):
    return os.path.join(MANIFEST_DIR, f"shard_{idx:03d}.json")


def write_shard_manifest(idx, data):
    os.makedirs(MANIFEST_DIR, exist_ok=True)
    with open(shard_manifest_path(idx), "w") as f:
        json.dump(data, f)


# ------------------------------------------------------------
# PHASE A — parallel enumeration via day dirs from summary.txt
# ------------------------------------------------------------

def load_day_dirs():
    day_dirs = []
    with open(SUMMARY_TXT) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("TOTAL"):
                continue
            date = line.split("|")[0].strip()
            day_dirs.append(os.path.join(SOURCE_DIR, date.replace("-", "/")))
    return day_dirs


def glob_worker(worker_id):
    r = redis.Redis(**REDIS)
    prefix = SOURCE_DIR.rstrip("/") + "/"

    while True:
        day_dir = r.spop(KEY_DIRS_WORK)

        if day_dir is None:
            print(f"Worker {worker_id}: queue empty, done", flush=True)
            return

        day_dir = day_dir.decode()

        files = glob.glob(os.path.join(day_dir, "*.json"))
        rel = [f[len(prefix):] for f in files]

        if rel:
            pipe = r.pipeline()
            for i, name in enumerate(rel, 1):
                pipe.rpush(KEY_FILES, name)
                if i % 10000 == 0:
                    pipe.execute()
            if len(rel) % 10000:
                pipe.execute()

        r.sadd(KEY_DIRS_DONE, day_dir)

        print(f"Worker {worker_id}: {day_dir} {len(files)} files", flush=True)


def snapshot(args):
    if args.dry_run:
        day_dirs = load_day_dirs()
        print(f"DRY RUN: {len(day_dirs)} day dirs from {SUMMARY_TXT} (expected {EXPECTED_DIRS})")
        return

    r = get_redis()

    if os.path.exists(FILES_TXT) and r.exists(KEY_FILES):
        print("Snapshot already loaded. Skipping.")
        return

    if not r.exists(KEY_DIRS_FILES):
        day_dirs = load_day_dirs()
        n = len(day_dirs)
        print(f"Loaded {n} day dirs from {SUMMARY_TXT}")

        if n != EXPECTED_DIRS:
            print(f"WARNING: expected {EXPECTED_DIRS} dirs, found {n}")

        pipe = r.pipeline()
        for d in day_dirs:
            pipe.sadd(KEY_DIRS_FILES, d)
        pipe.execute()
    else:
        print(f"Dir snapshot already loaded ({r.scard(KEY_DIRS_FILES)} dirs)")

    r.sdiffstore(KEY_DIRS_WORK, KEY_DIRS_FILES, KEY_DIRS_DONE)
    pending = r.scard(KEY_DIRS_WORK)

    print(f"{pending} dirs pending (skipping {r.scard(KEY_DIRS_DONE)} done)")

    if pending > 0:
        print(f"Globbing with {args.workers} workers ...")
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            list(executor.map(glob_worker, range(1, args.workers + 1)))
    else:
        print("No dirs to glob.")

    n = r.llen(KEY_FILES)
    print(f"Snapshot list {KEY_FILES} has {n} items")

    if n != EXPECTED_FILES:
        print(f"WARNING: expected {EXPECTED_FILES} files, found {n} (source may still be changing)")

    print(f"Writing {FILES_TXT} ...")

    with open(FILES_TXT, "w") as f:
        for start in range(0, n, 10000):
            items = r.lrange(KEY_FILES, start, start + 9999)
            for item in items:
                f.write(item.decode() + "\n")
            print(f"files.txt {start + len(items)}/{n}", flush=True)

    print(f"Done. files.txt has {n} lines")


# ------------------------------------------------------------
# PHASE B — working copy of the snapshot
# ------------------------------------------------------------

def ensure_work_list(r):
    if r.exists(KEY_WORK) and r.llen(KEY_WORK) > 0:
        print("Work list already exists. Skipping copy.")
        return

    if not r.exists(KEY_FILES):
        raise SystemExit("Snapshot missing. Run: upload_hf.py snapshot")

    total = r.llen(KEY_FILES)
    print(f"Copying {total} filenames to work list ...")

    r.delete(KEY_WORK_TMP)

    for start in range(0, total, 10000):
        items = r.lrange(KEY_FILES, start, start + 9999)
        pipe = r.pipeline()
        for item in items:
            pipe.rpush(KEY_WORK_TMP, item)
        pipe.execute()
        print(f"Copied {start + len(items)}/{total}", flush=True)

    r.rename(KEY_WORK_TMP, KEY_WORK)
    print(f"Work list ready ({r.llen(KEY_WORK)} items)")


# ------------------------------------------------------------
# PHASE C — build shards (multiprocess workers, copies files)
# ------------------------------------------------------------

def build_shard_worker(worker_id, use_pigz):
    r = redis.Redis(**REDIS)
    total_files = EXPECTED_FILES
    worker_start = time.monotonic()

    while True:
        pipe = r.pipeline()
        for _ in range(FILES_PER_SHARD):
            pipe.lpop(KEY_WORK)

        batch = [
            item.decode()
            for item in pipe.execute()
            if item is not None
        ]

        if not batch:
            print(f"Worker {worker_id}: queue empty, done", flush=True)
            return

        idx = r.incr(KEY_SEQ)
        name = f"shard_{idx:03d}.tar.gz"
        tarball = os.path.join(DATA_DIR, name)
        partial = tarball + ".partial"
        listing = os.path.join(DATA_DIR, f"shard_{idx:03d}.files")
        mpath = shard_manifest_path(idx)

        manifest = {}
        if os.path.exists(mpath):
            with open(mpath) as f:
                manifest = json.load(f)

        if os.path.exists(tarball) and manifest.get("built"):
            print(f"Worker {worker_id}: {name} already built, skipping", flush=True)
            continue

        if os.path.exists(listing):
            print(
                f"Worker {worker_id}: resuming {name} from {os.path.basename(listing)} ...",
                flush=True,
            )
            with open(listing) as f:
                batch = [line.strip() for line in f if line.strip()]
        else:
            with open(listing, "w") as f:
                f.write("\n".join(batch) + "\n")

        for stale in (tarball, partial):
            if os.path.exists(stale):
                os.remove(stale)

        print(f"Worker {worker_id}: building {name} ({len(batch)} files) ...", flush=True)
        start = time.monotonic()

        raw_bytes = 0
        for path in batch:
            raw_bytes += os.path.getsize(os.path.join(SOURCE_DIR, path))

        if use_pigz:
            tar_cmd = ["tar", "--use-compress-program=pigz", "--sort=name", "-cvf", partial, "-C", SOURCE_DIR, "-T", listing]
        else:
            tar_cmd = ["tar", "--sort=name", "-czvf", partial, "-C", SOURCE_DIR, "-T", listing]

        proc = subprocess.Popen(
            tar_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stderr_buf = []
        threading.Thread(
            target=lambda: [stderr_buf.append(line) for line in proc.stderr],
            daemon=True,
        ).start()

        packed = 0
        pipe = r.pipeline()
        for line in proc.stdout:
            packed += 1
            pipe.incr(KEY_PACKED)
            if packed % 1000 == 0:
                pipe.execute()
        proc.stdout.close()
        if packed % 1000:
            pipe.execute()

        proc.wait()
        err = "".join(stderr_buf)

        if proc.returncode != 0:
            print(f"Worker {worker_id}: tar failed for {name}: {err.strip()[:500]}", flush=True)
            raise SystemExit(1)

        if packed == 0:
            print(
                f"Worker {worker_id}: WARNING {name} built but 0 files counted "
                "(tar -v listing not on stdout)",
                flush=True,
            )

        os.replace(partial, tarball)
        os.remove(listing)

        gz_bytes = os.path.getsize(tarball)

        write_shard_manifest(
            idx,
            {
                "built": True,
                "uploaded": False,
                "tarball": name,
                "files": len(batch),
                "raw_bytes": raw_bytes,
                "gz_bytes": gz_bytes,
            },
        )

        elapsed = time.monotonic() - start

        done_files = built_files_count()
        remaining = total_files - done_files
        worker_elapsed = time.monotonic() - worker_start
        rate = done_files / worker_elapsed if worker_elapsed > 0 else 0
        if rate > 0:
            eta = fmt_eta(remaining / rate)
        else:
            eta = "N/A"

        print(
            f"Worker {worker_id}: {name} {len(batch)} files "
            f"{raw_bytes / (1024 * 1024):.0f}MB -> {gz_bytes / (1024 * 1024):.0f}MB in {elapsed:.1f}s | "
            f"{done_files:,}/{total_files:,} files ({done_files / total_files * 100:.0f}%), "
            f"{rate:.0f} files/s, ETA {eta}",
            flush=True,
        )


def shards(args):
    r = get_redis()

    if args.reset:
        print("Resetting shard state ...")
        for d in (DATA_DIR, MANIFEST_DIR):
            if os.path.isdir(d):
                for f in os.listdir(d):
                    os.remove(os.path.join(d, f))
        r.delete(KEY_WORK, KEY_WORK_TMP, KEY_SEQ, KEY_PACKED)

    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(MANIFEST_DIR, exist_ok=True)

    use_pigz = args.pigz and shutil.which("pigz") is not None
    if args.pigz and not use_pigz:
        print("WARNING: pigz not found, falling back to gzip")

    ensure_work_list(r)

    print(f"Building shards with {args.workers} workers ...")

    total_files = EXPECTED_FILES
    total_shards = math.ceil(total_files / FILES_PER_SHARD)
    stop = threading.Event()

    def ticker():
        tick_start = time.monotonic()
        prev_done = 0
        prev_time = None
        samples = []

        while not stop.wait(5):
            done = int(r.get(KEY_PACKED) or 0)
            now = time.monotonic()
            remaining = total_files - done
            built = built_files_count()
            in_flight = done - built
            done_shards = built // FILES_PER_SHARD

            if prev_time is None:
                prev_time = now
                prev_done = done

            samples.append((now, done))
            while samples and now - samples[0][0] > 30:
                samples.pop(0)

            if len(samples) >= 2 and samples[-1][0] > samples[0][0]:
                rate = (samples[-1][1] - samples[0][1]) / (samples[-1][0] - samples[0][0])
            else:
                rate = (done - prev_done) / (now - prev_time) if now > prev_time else 0

            prev_time = now
            prev_done = done
            eta = fmt_eta_long(remaining / rate) if rate > 0 else "N/A"

            print(
                f"Progress: {done_shards}/{total_shards} shards complete, "
                f"{done:,}/{total_files:,} files ({done / total_files * 100:.0f}%), "
                f"{in_flight:,} in-flight, ~{rate:.0f} files/s, ETA {eta}",
                flush=True,
            )

    t = threading.Thread(target=ticker, daemon=True)
    t.start()

    try:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            for _ in executor.map(
                build_shard_worker,
                range(1, args.workers + 1),
                [use_pigz] * args.workers,
            ):
                pass
    finally:
        stop.set()

    merged = {}

    for name in sorted(os.listdir(MANIFEST_DIR)):
        if name.endswith(".json"):
            with open(os.path.join(MANIFEST_DIR, name)) as f:
                entry = json.load(f)
            merged[entry["tarball"]] = entry

    with open(MERGED_MANIFEST, "w") as f:
        json.dump(merged, f, indent=2)

    total_files = sum(e["files"] for e in merged.values())
    print(f"Shards done: {len(merged)} shards, {total_files} files")

    if total_files != EXPECTED_FILES:
        print(f"WARNING: expected {EXPECTED_FILES} files, got {total_files}")


# ------------------------------------------------------------
# PHASE D — upload shards to Hugging Face (resume-friendly)
# ------------------------------------------------------------

def write_readme():
    count = 0
    if os.path.exists(FILES_TXT):
        with open(FILES_TXT) as f:
            count = sum(1 for _ in f)

    content = (
        "---\n"
        "license: other\n"
        "---\n"
        "\n"
        "# Raw tweets\n"
        "\n"
        f"{count} raw Twitter API v2 JSON files, compressed into gzipped shards under `data/`.\n"
        "\n"
        "Extract a shard with:\n"
        "    tar -xzf shard_000.tar.gz\n"
        "\n"
        "Collection method and license TBD.\n"
    )

    with open(README_MD, "w") as f:
        f.write(content)


def upload(args):
    try:
        from huggingface_hub import HfApi
    except ImportError:
        sys.exit("huggingface_hub not installed. Run: .venv/bin/pip install huggingface_hub")

    token = os.environ.get("HF_TOKEN")

    if not token:
        cached = os.path.expanduser("~/.cache/huggingface/token")
        if os.path.exists(cached):
            with open(cached) as f:
                token = f.read().strip()

    if not token:
        sys.exit("HF_TOKEN env var or ~/.cache/huggingface/token is required")

    api = HfApi(token=token)
    repo_id = args.repo_id

    try:
        api.repo_info(repo_id=repo_id, repo_type="dataset")
    except Exception:
        print(f"Creating repo {repo_id} ...")
        api.create_repo(repo_id=repo_id, repo_type="dataset", private=args.private)

    write_readme()

    names = sorted(n for n in os.listdir(DATA_DIR) if n.endswith(".tar.gz"))

    for name in names:
        idx = int(name.split("_")[1].split(".")[0])
        mpath = shard_manifest_path(idx)

        entry = {}
        if os.path.exists(mpath):
            with open(mpath) as f:
                entry = json.load(f)

        if entry.get("uploaded"):
            print(f"Skipping {name} (already uploaded)")
            continue

        print(f"Uploading {name} ...", flush=True)

        api.upload_file(
            path_or_fileobj=os.path.join(DATA_DIR, name),
            path_in_repo=f"data/{name}",
            repo_id=repo_id,
            repo_type="dataset",
        )

        entry["uploaded"] = True
        write_shard_manifest(idx, entry)
        os.remove(os.path.join(DATA_DIR, name))
        print(f"Uploaded {name}, removed local copy", flush=True)

    api.upload_file(
        path_or_fileobj=README_MD,
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
    )

    api.upload_file(
        path_or_fileobj=MERGED_MANIFEST,
        path_in_repo="manifest.json",
        repo_id=repo_id,
        repo_type="dataset",
    )

    print("Upload complete")


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Snapshot, shard and upload raw tweets to Hugging Face")
    sub = parser.add_subparsers(dest="command")

    p_snapshot = sub.add_parser("snapshot", help="load day dirs and glob filenames into redis")
    p_snapshot.add_argument("--workers", type=int, default=4)
    p_snapshot.add_argument("--dry-run", action="store_true")
    p_snapshot.set_defaults(func=snapshot)

    p_shards = sub.add_parser("shards", help="build gzipped shards from the snapshot")
    p_shards.add_argument("--workers", type=int, default=4)
    p_shards.add_argument("--reset", action="store_true", help="wipe existing shard state and rebuild the work list")
    p_shards.add_argument("--pigz", action="store_true", help="use pigz for parallel compression (falls back to gzip)")
    p_shards.set_defaults(func=shards)

    p_upload = sub.add_parser("upload", help="upload shards to Hugging Face")
    p_upload.add_argument("--repo-id", default=DEFAULT_REPO)
    p_upload.add_argument("--private", action="store_true")
    p_upload.set_defaults(func=upload)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    args.func(args)


if __name__ == "__main__":
    main()
