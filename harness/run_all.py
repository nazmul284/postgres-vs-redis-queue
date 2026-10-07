"""Every backend x tier x test x rep, interleaved by rep. Appends to results/runs.jsonl; resumable.
Postgres 'strict' runs with the server's default wal_sync_method (open_datasync on macOS);
'strict_wt' sets wal_sync_method = fsync_writethrough first, matching Redis's F_FULLFSYNC."""
import json, subprocess, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent; PY = str(ROOT / ".venv" / "bin" / "python")
COMBOS = [("pg", "strict"), ("pg", "strict_wt"), ("pg", "relaxed"),
          ("redis_list", "strict"), ("redis_list", "relaxed"), ("redis_list", "default"),
          ("redis_stream", "strict"), ("redis_stream", "relaxed"), ("redis_stream", "default")]
TESTS = [("enqueue", 1), ("enqueue", 4), ("drain", 1), ("drain", 4), ("drain", 8), ("latency", 4)]
def wal(method):
    subprocess.run(["psql", "-h", "localhost", "-p", "5499", "-U", "bench", "-d", "postgres", "-q",
                    "-c", f"alter system set wal_sync_method = '{method}'", "-c", "select pg_reload_conf()"], capture_output=True)
out = ROOT / "results" / "runs.jsonl"
done = {(j["backend"], j["tier"], j["test"], j["param"], j["rep"]) for j in map(json.loads, out.open())} if out.exists() else set()
for rep in (1, 2, 3):
    for b, t in COMBOS:
        wal("fsync_writethrough" if t == "strict_wt" else "open_datasync")
        for test, param in TESTS:
            if (b, t, test, param, rep) in done:
                continue
            p = subprocess.run([PY, str(ROOT / "harness" / "jobq.py"), b, t, test, str(param), str(rep)], capture_output=True, text=True)
            line = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else json.dumps(
                {"backend": b, "tier": t, "test": test, "param": param, "rep": rep, "error": p.stderr[-300:]})
            with out.open("a") as f:
                f.write(line + "\n")
            print(line[:160], flush=True)
wal("open_datasync")
