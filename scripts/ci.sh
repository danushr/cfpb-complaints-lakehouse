#!/bin/sh
set -eu
LAKE_DATA_DIR=$(mktemp -d)
export LAKE_DATA_DIR
trap 'rm -rf "$LAKE_DATA_DIR"' EXIT

uv run pytest -q
uv run python -m lakehouse.run --csv "$LAKE_PROJECT_DIR/sample/complaints_v1.csv" --batch-id v1
uv run python -m lakehouse.run --csv "$LAKE_PROJECT_DIR/sample/complaints_v2.csv" --batch-id v2 | tee "$LAKE_DATA_DIR/v2.log"
grep -q "silver: 319 inserted, 40 updated" "$LAKE_DATA_DIR/v2.log"
