#!/usr/bin/env bash

set -euo pipefail

REPO_ID="${REPO_ID:-YOUR_USERNAME/tweets-raw}"
HF_TOKEN="${HF_TOKEN:-}"

SOURCE_DIR="/mnt/data/o365/sorted_tweets"
STAGE_DIR="/mnt/data/projects/million-downloader/hf_stage"
TMP_DIR="/mnt/data/projects/million-downloader/hf_tmp"

FILES_PER_SHARD=100000
PRIVATE=false

VENV="/mnt/data/projects/million-downloader/.venv"

if ! "$VENV/bin/python3" -c "import huggingface_hub" 2>/dev/null; then
    echo "Installing huggingface_hub..."
    "$VENV/bin/pip" install -q huggingface_hub
fi

if [ -n "$HF_TOKEN" ]; then
    export HF_TOKEN
elif [ -f "$HOME/.cache/huggingface/token" ]; then
    export HF_TOKEN="$(cat "$HOME/.cache/huggingface/token")"
else
    "$VENV/bin/huggingface-cli" login
    export HF_TOKEN="$(cat "$HOME/.cache/huggingface/token")"
fi

echo "Creating dataset repo $REPO_ID (private=$PRIVATE) if needed..."
curl -sS -X POST https://huggingface.co/api/repos/create \
    -H "Authorization: Bearer $HF_TOKEN" \
    -d "{\"type\":\"dataset\",\"name\":\"${REPO_ID#*/}\",\"private\":$PRIVATE}" \
    || true

echo "Building shards from $SOURCE_DIR..."
rm -rf "$TMP_DIR" "$STAGE_DIR"
mkdir -p "$TMP_DIR" "$STAGE_DIR/shards"

find "$SOURCE_DIR" -type f -name '*.json' | sort > "$TMP_DIR/files.txt"

TOTAL_FILES=$(wc -l < "$TMP_DIR/files.txt")
echo "Found $TOTAL_FILES files"

split -l "$FILES_PER_SHARD" -d -a 3 "$TMP_DIR/files.txt" "$TMP_DIR/chunk_"

i=0
for chunk in "$TMP_DIR"/chunk_*; do
    shard="$(printf 'shard_%03d.tar.gz' "$i")"
    echo "Creating $shard ..."
    tar -czf "$STAGE_DIR/shards/$shard" -C "$SOURCE_DIR" -T "$chunk"
    i=$((i + 1))
done

cat > "$STAGE_DIR/README.md" <<EOF
---
license: other
---
# Raw tweets

$TOTAL_FILES raw tweet JSON files in \`shard_*.tar.gz\`.

Collection method and license TBD.
EOF

echo "Uploading $i shards to $REPO_ID ..."
"$VENV/bin/huggingface-cli" upload "$REPO_ID" "$STAGE_DIR" \
    --repo-type dataset \
    --commit-message "raw tweet json shards"

echo "Cleaning up..."
rm -rf "$TMP_DIR" "$STAGE_DIR"

echo "Done. Uploaded $TOTAL_FILES files across $i shards."
