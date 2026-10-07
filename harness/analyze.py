"""Medians over reps from results/runs.jsonl -> results/summary.json + tables.txt.
Also compares Redis with and without hiredis (results/runs_redis_without_hiredis.jsonl)."""
import json, statistics as st
from collections import defaultdict
from pathlib import Path
R = Path(__file__).resolve().parent.parent / "results"
rs = [json.loads(l) for l in (R / "runs.jsonl").open() if "error" not in l]
g = defaultdict(list)
for r in rs:
    g[(r["backend"], r["tier"], r["test"], r["param"])].append(r)
med = lambda xs: round(st.median(xs), 2)
summ = {}
for k, v in g.items():
    key = "|".join(map(str, k))
    if k[2] == "latency":
        summ[key] = {"reps": len(v), "p50_ms": med([r["p50_ms"] for r in v]), "p99_ms": med([r["p99_ms"] for r in v]),
                     "done_share": med([r["jobs"] / r["sent"] for r in v])}
    else:
        summ[key] = {"reps": len(v), "jobs_per_s": round(st.median([r["jobs_per_s"] for r in v])),
                     "min": min(r["jobs_per_s"] for r in v), "max": max(r["jobs_per_s"] for r in v)}
(R / "summary.json").write_text(json.dumps(summ, indent=1))
NAME = {"pg": "Postgres SKIP LOCKED", "redis_list": "Redis list", "redis_stream": "Redis stream"}
def row(b, t, test, p):
    v = summ.get(f"{b}|{t}|{test}|{p}")
    return f"{v['jobs_per_s']:,}" if v else "-"
L = ["Jobs per second, median of 3 runs", "",
      "Durability: up to ~1 s of jobs at risk on a crash",
      "(Postgres synchronous_commit=off, Redis appendfsync everysec)", "",
      f"{'':22}{'enqueue':>9}{'enqueue':>9}{'drain':>8}{'drain':>8}{'drain':>8}",
      f"{'':22}{'1 prod':>9}{'4 prod':>9}{'1 wkr':>8}{'4 wkr':>8}{'8 wkr':>8}",
      f"{'-'*21:22}{'-'*7:>9}{'-'*7:>9}{'-'*6:>8}{'-'*6:>8}{'-'*6:>8}"]
for b in ("pg", "redis_list", "redis_stream"):
    L.append(f"{NAME[b]:22}{row(b,'relaxed','enqueue',1):>9}{row(b,'relaxed','enqueue',4):>9}{row(b,'relaxed','drain',1):>8}{row(b,'relaxed','drain',4):>8}{row(b,'relaxed','drain',8):>8}")
L += ["", "", "Durability: every job flushed to disk before it is acknowledged",
      "(Postgres fsync_writethrough, Redis appendfsync always; both F_FULLFSYNC on macOS)", "",
      f"{'':22}{'enqueue':>9}{'enqueue':>9}{'drain':>8}{'drain':>8}{'drain':>8}",
      f"{'':22}{'1 prod':>9}{'4 prod':>9}{'1 wkr':>8}{'4 wkr':>8}{'8 wkr':>8}",
      f"{'-'*21:22}{'-'*7:>9}{'-'*7:>9}{'-'*6:>8}{'-'*6:>8}{'-'*6:>8}"]
for b, t in (("pg", "strict_wt"), ("redis_list", "strict"), ("redis_stream", "strict")):
    L.append(f"{NAME[b]:22}{row(b,t,'enqueue',1):>9}{row(b,t,'enqueue',4):>9}{row(b,t,'drain',1):>8}{row(b,t,'drain',4):>8}{row(b,t,'drain',8):>8}")
L.append(f"{'PG, macOS default*':22}{row('pg','strict','enqueue',1):>9}{row('pg','strict','enqueue',4):>9}{row('pg','strict','drain',1):>8}{row('pg','strict','drain',4):>8}{row('pg','strict','drain',8):>8}")
L += ["* wal_sync_method=open_datasync, which on macOS does not flush the drive cache", "", "",
      "Pickup latency at a steady 500 jobs/s, 4 workers (median of 3 runs)", "",
      f"{'':22}{'durability':>13}{'p50 ms':>9}{'p99 ms':>9}{'kept up':>9}",
      f"{'-'*21:22}{'-'*11:>13}{'-'*6:>9}{'-'*6:>9}{'-'*7:>9}"]
for b, t, lab in (("pg", "relaxed", "~1 s"), ("redis_list", "relaxed", "~1 s"), ("redis_stream", "relaxed", "~1 s"),
                  ("pg", "strict_wt", "flushed"), ("redis_list", "strict", "flushed"), ("redis_stream", "strict", "flushed")):
    v = summ.get(f"{b}|{t}|latency|4")
    if v:
        kept = "yes" if v["done_share"] > .99 else f"{v['done_share']:.0%}"
        L.append(f"{NAME[b]:22}{lab:>13}{v['p50_ms']:>9}{v['p99_ms']:>9}{kept:>9}")
# hiredis effect
nh = R / "runs_redis_without_hiredis.jsonl"
if nh.exists():
    old = [json.loads(l) for l in nh.open()]
    L += ["", "", "redis-py with and without the hiredis parser, relaxed tier, rep 1 (jobs/s)", "",
          f"{'':26}{'without':>10}{'with':>10}", f"{'-'*25:26}{'-'*8:>10}{'-'*8:>10}"]
    for b in ("redis_list", "redis_stream"):
        for test, p in (("enqueue", 1), ("drain", 8)):
            o = next((r["jobs_per_s"] for r in old if r["backend"] == b and r["tier"] == "relaxed" and r["test"] == test and r["param"] == p and r["rep"] == 1), None)
            n = next((r["jobs_per_s"] for r in rs if r["backend"] == b and r["tier"] == "relaxed" and r["test"] == test and r["param"] == p and r["rep"] == 1), None)
            if o and n:
                L.append(f"{NAME[b] + ' ' + test + ' ' + str(p):26}{o:>10,}{n:>10,}")
(R / "tables.txt").write_text("\n".join(L) + "\n"); print("\n".join(L))
