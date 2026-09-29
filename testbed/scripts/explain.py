import json, sys
d = json.load(sys.stdin)
print("  summary:", d["summary"])
for s in d["segments"]:
    print(f"  {s['segment_id']} {s['mode']} {s['reason']} candidates={s['candidates']}")
