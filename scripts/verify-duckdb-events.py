#!/usr/bin/env python3
"""
Smoke-check a DuckDB file written by opik-openclaw (TruLens-compatible `trulens_events`).

Usage:
  python scripts/verify-duckdb-events.py path/to/opik-openclaw.trulens.duckdb

Requires: pip install duckdb
Optional (TruLens ORM path): pip install trulens-core sqlalchemy  # then use DefaultDBConnector in your own code
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: verify-duckdb-events.py <path-to.duckdb>", file=sys.stderr)
        return 2
    path = sys.argv[1]
    try:
        import duckdb
    except ImportError:
        print("install duckdb: pip install duckdb", file=sys.stderr)
        return 1

    con = duckdb.connect(path, read_only=True)
    try:
        n = con.execute("select count(*) from trulens_events").fetchone()[0]
        print(f"trulens_events rows: {n}")
        sample = con.execute(
            "select event_id, record_type, timestamp from trulens_events order by timestamp desc limit 3"
        ).fetchall()
        for row in sample:
            print("  ", row)
        # JSON columns may come back as dict or str depending on duckdb version
        one = con.execute(
            "select record, trace from trulens_events limit 1"
        ).fetchone()
        if one:
            rec, tr = one[0], one[1]
            if isinstance(rec, str):
                rec = json.loads(rec)
            if isinstance(tr, str):
                tr = json.loads(tr)
            print("sample record keys:", list(rec.keys()) if isinstance(rec, dict) else type(rec))
            print("sample trace keys:", list(tr.keys()) if isinstance(tr, dict) else type(tr))
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
