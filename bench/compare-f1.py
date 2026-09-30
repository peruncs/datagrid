#!/usr/bin/env python3
"""Fail when the production F1 type scan is over 5% slower than Serializer."""

import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} JMH_RESULTS.json", file=sys.stderr)
        return 2

    results = json.loads(Path(sys.argv[1]).read_text())
    scores = {
        (item["benchmark"].rsplit(".", 1)[-1], int(item["params"]["payloadBytes"])):
            item["primaryMetric"]["score"]
        for item in results
    }
    failed = False
    for size in (65_536, 1_048_576):
        raw = scores.get(("serializerRawHeaderScan", size))
        checked = scores.get(("boundsCheckedProductionBinaryHeaderScan", size))
        if raw is None or checked is None:
            print(f"missing F1 result for {size} bytes", file=sys.stderr)
            return 2
        ratio = checked / raw
        print(f"{size // 1024:>4} KiB: checked/raw={ratio:.4f} ({(1 - ratio) * 100:+.2f}% vs raw)")
        failed |= ratio < 0.95
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
