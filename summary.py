#!/usr/bin/env python3

import glob
import os

BASE = "/mnt/data/o365/sorted_tweets"

OUT_FILE = "/mnt/data/projects/million-downloader/summary.txt"


def main():

    day_dirs = glob.glob(os.path.join(BASE, "*", "*", "*"))

    day_dirs.sort()

    lines = []

    total = 0

    for day_dir in day_dirs:

        if not os.path.isdir(day_dir):
            continue

        count = len(
            glob.glob(os.path.join(day_dir, "*.json"))
        )

        date = "-".join(day_dir.split("/")[-3:])

        line = f"{date} | {count}"

        lines.append(line)

        print(line)

        total += count

    total_line = f"TOTAL | {total} files across {len(day_dirs)} days"

    print(total_line)

    lines.append(total_line)

    with open(OUT_FILE, "w") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
