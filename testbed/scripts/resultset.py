#!/usr/bin/env python3
"""Print a query response's result set in a canonical form, for the correctness gates.

    resultset.py loki|jsonl <response-file>

One line per record, "<unix-nanoseconds>\t<line as a JSON string>", sorted. Two engines
returned the same answer exactly when this output is byte-identical: the timestamp is
normalised to integer nanoseconds (Loki sends a nanosecond string, VictoriaLogs' LogsQL an
RFC3339 `_time`), the line is JSON-quoted so a record is always one output line, and the
sort removes stream grouping and order, which legitimately differ between engines.

Formats: `loki` is Loki's query_range JSON (logd answers the same shape); `jsonl` is one
JSON object per line with `_time` and `_msg`, VictoriaLogs' /select/logsql/query output.
An error response exits non-zero with the body's start on stderr.
"""
import json
import re
import sys
from datetime import datetime

RFC3339 = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)")


def ns(ts):
    if isinstance(ts, str) and ts.isdigit():
        return int(ts)
    m = RFC3339.fullmatch(ts)
    if not m:
        raise ValueError(f"unrecognised timestamp {ts!r}")
    zone = "+00:00" if m[3] == "Z" else m[3]
    secs = int(datetime.fromisoformat(m[1] + zone).timestamp())
    return secs * 10**9 + int((m[2] or "0").ljust(9, "0"))


def records(fmt, text):
    if fmt == "loki":
        d = json.loads(text)
        if d.get("status") != "success":
            raise ValueError(f"not a success response: {text[:300]}")
        for r in d["data"]["result"]:
            for ts, line in r["values"]:
                yield ns(ts), line
    elif fmt == "jsonl":
        for raw in text.splitlines():
            if raw.strip():
                o = json.loads(raw)
                yield ns(o["_time"]), o["_msg"]
    else:
        raise ValueError(f"unknown format {fmt!r}")


def canonical(fmt, text):
    return sorted(f"{t}\t{json.dumps(line, ensure_ascii=False)}" for t, line in records(fmt, text))


def self_test():
    loki = json.dumps({"status": "success", "data": {"resultType": "streams", "result": [
        {"stream": {"app": "a"}, "values": [["1700000000123456789", "took=5ms"], ["1700000001000000000", "b\tc"]]},
        {"stream": {"app": "b"}, "values": [["1700000000000000001", 'q"uote']]},
    ]}})
    jsonl = "\n".join(json.dumps(o) for o in [
        {"_time": "2023-11-14T22:13:21Z", "_msg": "b\tc", "_stream": "{app=\"a\"}"},
        {"_time": "2023-11-14T22:13:20.000000001Z", "_msg": 'q"uote'},
        {"_time": "2023-11-14T23:13:20.123456789+01:00", "_msg": "took=5ms"},
    ]) + "\n"
    a, b = canonical("loki", loki), canonical("jsonl", jsonl)
    assert a == b, (a, b)
    assert a[0] == '1700000000000000001\t"q\\"uote"', a[0]
    assert canonical("jsonl", jsonl.replace("took=5ms", "took=6ms")) != a
    try:
        canonical("loki", '{"status":"error","error":"boom"}')
        raise AssertionError("an error response must not canonicalise")
    except ValueError:
        pass
    print("resultset self-test ok")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        sys.exit(0)
    fmt, path = sys.argv[1], sys.argv[2]
    try:
        with open(path, encoding="utf-8") as f:
            out = canonical(fmt, f.read())
    except (ValueError, KeyError, TypeError) as e:
        print(f"{path}: {e}", file=sys.stderr)
        sys.exit(1)
    sys.stdout.write("".join(line + "\n" for line in out))
