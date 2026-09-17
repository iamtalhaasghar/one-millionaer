#!/usr/bin/env bash
set -euo pipefail

TARGET="${1:-/mnt/data/o365/tweets}"
POLL="${POLL:-5}"

case "$(realpath -m "$TARGET")" in
    /mnt/data/o365/tweets) ;;
    *) echo "ERROR: refusing to nuke unexpected path: $TARGET" >&2; exit 1 ;;
esac

[[ -d "$TARGET" ]] || { echo "ERROR: $TARGET is not a directory" >&2; exit 1; }

# Rough bytes per directory entry on ext4 (name + htree slot). Display only;
# percent/ETA come from the real byte counts.
BYTES_PER_ENTRY="${BYTES_PER_ENTRY:-48}"

SIZE_TOTAL="$(stat -c%s "$TARGET")"
echo "Nuking $TARGET (${SIZE_TOTAL} dir bytes) ..."

start="$(date +%s)"
rm -rf "$TARGET" &
RM_PID=$!

prev="$SIZE_TOTAL"
prev_t="$start"
while kill -0 "$RM_PID" 2>/dev/null; do
    sleep "$POLL"
    now="$(stat -c%s "$TARGET" 2>/dev/null || echo 0)"
    now_t="$(date +%s)"
    dt=$((now_t - prev_t)); rate=0
    [[ $dt -gt 0 ]] && rate=$(((prev - now) / dt))
    freed=$((SIZE_TOTAL - now))
    pct=$(( SIZE_TOTAL > 0 ? freed * 100 / SIZE_TOTAL : 0 ))
    entries_done=$((freed / BYTES_PER_ENTRY))
    eta="?"
    if [[ $rate -gt 0 ]]; then
        eta_s=$((now / rate))
        eta="$(printf '%d:%02d:%02d' $((eta_s/3600)) $(((eta_s/60)%60)) $((eta_s%60)))"
    fi
    elapsed=$((now_t - start))
    printf "\r[%3d%%] ~%d entries done | %d KB/s | ETA %s | %d:%02d elapsed" \
        "$pct" "$entries_done" "$((rate/1024))" "$eta" $((elapsed/60)) $((elapsed%60))
    prev="$now"; prev_t="$now_t"
done
wait "$RM_PID"

mkdir -p "$TARGET"
printf "\nDone.\n"
