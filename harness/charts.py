"""Two panels: throughput at ~1 s durability and fully flushed. usage: uv run --with matplotlib python charts.py <out.png>"""
import json, sys
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
s = json.loads((Path(__file__).resolve().parent.parent / "results" / "summary.json").read_text())
B = [("pg", "#2563eb", "Postgres SKIP LOCKED"), ("redis_list", "#dc2626", "Redis list"), ("redis_stream", "#f59e0b", "Redis stream")]
TESTS = [("enqueue", 1, "enqueue,\n1 producer"), ("enqueue", 4, "enqueue,\n4 producers"), ("drain", 1, "drain,\n1 worker"), ("drain", 8, "drain,\n8 workers")]
fig, axes = plt.subplots(1, 2, figsize=(10.5, 5.4), dpi=200)
for ax, tiers, title in ((axes[0], {"pg": "relaxed", "redis_list": "relaxed", "redis_stream": "relaxed"}, "Up to ~1 s of jobs at risk on a crash"),
                         (axes[1], {"pg": "strict_wt", "redis_list": "strict", "redis_stream": "strict"}, "Every job flushed to disk first")):
    for i, (test, p, _) in enumerate(TESTS):
        for j, (b, col, _) in enumerate(B):
            v = s[f"{b}|{tiers[b]}|{test}|{p}"]["jobs_per_s"] / 1000
            ax.bar(i + (j - 1) * 0.27, v, width=0.26, color=col, zorder=2)
    ax.set_xticks(range(len(TESTS)), [t[2] for t in TESTS], fontsize=9)
    ax.set_title(title, fontsize=10.5, color="#1f2328", loc="left")
    for sp in ("top", "right"): ax.spines[sp].set_visible(False)
    ax.tick_params(colors="#6b7280"); ax.grid(axis="y", color="#eeeeee", zorder=0)
axes[0].set_ylabel("thousand jobs per second (median of 3)", fontsize=9.5, color="#6b7280")
axes[1].set_ylabel("thousand jobs per second", fontsize=9.5, color="#6b7280")
fig.legend(handles=[Patch(color=c, label=l) for _, c, l in B], loc="lower center", ncol=3, frameon=False, fontsize=10, bbox_to_anchor=(0.5, 0.06))
fig.text(0.02, 0.96, "Postgres enqueued faster; Redis lists drained faster. Flushed, all three crawl.", fontsize=13, weight="bold", color="#1f2328")
fig.text(0.02, 0.915, "Python clients (psycopg 3, redis-py with hiredis), localhost, Postgres 16.15, Redis 8.10.2. Note the different y-axis scales.",
         fontsize=9.5, color="#6b7280")
fig.text(0.02, 0.012, "Apple M2, 8 GB · 20,000 jobs per run (4,000 when flushed) · github.com/nazmul284/postgres-vs-redis-queue", fontsize=6.8, color="#6b7280")
fig.subplots_adjust(left=0.07, right=0.98, top=0.84, bottom=0.25, wspace=0.25)
fig.savefig(sys.argv[1], facecolor="white"); print("ok")
