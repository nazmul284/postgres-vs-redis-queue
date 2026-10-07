"""Postgres vs Redis as a job queue. One measurement per invocation; prints a JSON line.

usage: python jobq.py <backend> <tier> <test> <param> <rep>
  backend: pg | redis_list | redis_stream
  tier:    strict  (PG synchronous_commit=on  | Redis appendonly yes, appendfsync always)
           relaxed (PG synchronous_commit=off | Redis appendonly yes, appendfsync everysec)
           default (Redis appendonly no, Redis's out-of-the-box setting; not run for PG, whose default is strict)
  test:    enqueue <producers> | drain <workers> | latency <workers>
"""
import json, multiprocessing as mp, os, statistics as st, sys, time

PG_DSN = "host=localhost port=5499 user=bench dbname=queue"
RP = 6399
N_FAST, N_STRICT = 20_000, 4_000                # fully flushed writes run at a few hundred a second
RATE, SECONDS = 500, 15
PAYLOAD = {"task": "send_email", "to": "user{}@example.com", "template": "welcome",
           "args": {"plan": "pro", "locale": "en-GB", "trial_days": 14, "ref": "campaign-autumn-2026"}}

def payload(i):
    p = dict(PAYLOAD); p["to"] = p["to"].format(i); p["enqueued_at"] = time.time(); return p

# ------------------------------------------------------------------ backends
class PG:
    def __init__(self, tier):
        import psycopg
        self.c = psycopg.connect(PG_DSN, autocommit=True)
        self.c.execute(f"set synchronous_commit = {'on' if tier.startswith('strict') else 'off'}")
    @staticmethod
    def reset(tier):
        import psycopg
        with psycopg.connect("host=localhost port=5499 user=bench dbname=postgres", autocommit=True) as c:
            if not c.execute("select 1 from pg_database where datname='queue'").fetchone():
                c.execute("create database queue")
        with psycopg.connect(PG_DSN, autocommit=True) as c:
            c.execute("drop table if exists jobs")
            c.execute("create table jobs (id bigserial primary key, payload jsonb not null)")
    def enqueue(self, p):
        self.c.execute("with j as (insert into jobs (payload) values (%s) returning id) select pg_notify('jobs', '') from j",
                       (json.dumps(p),))
    def work_one(self, wait):
        """Claim, process, delete, commit. Returns the payload or None when the queue is empty."""
        with self.c.transaction():
            row = self.c.execute("select id, payload from jobs order by id for update skip locked limit 1").fetchone()
            if row:
                self.c.execute("delete from jobs where id = %s", (row[0],))
                return row[1]
        if wait:
            self.c.execute("listen jobs")
            for _ in self.c.notifies(timeout=0.5, stop_after=1):
                pass
        return None

class RedisBase:
    def __init__(self, tier):
        import redis
        self.r = redis.Redis(port=RP)
    @staticmethod
    def configure(tier):
        import redis
        r = redis.Redis(port=RP)
        r.flushall()
        if tier == "default":
            r.config_set("appendonly", "no")
        else:
            r.config_set("appendonly", "yes")
            r.config_set("appendfsync", "always" if tier.startswith("strict") else "everysec")
        time.sleep(0.5)

class RedisList(RedisBase):
    @staticmethod
    def reset(tier):
        RedisBase.configure(tier)
    def enqueue(self, p):
        self.r.lpush("jobs", json.dumps(p))
    def work_one(self, wait):
        if wait:
            got = self.r.brpop("jobs", timeout=0.5)
            return json.loads(got[1]) if got else None
        got = self.r.rpop("jobs")
        return json.loads(got) if got else None

class RedisStream(RedisBase):
    def __init__(self, tier):
        super().__init__(tier); self.name = f"c{os.getpid()}"
    @staticmethod
    def reset(tier):
        import redis
        RedisBase.configure(tier)
        redis.Redis(port=RP).xgroup_create("jobs", "workers", id="0", mkstream=True)
    def enqueue(self, p):
        self.r.xadd("jobs", {"p": json.dumps(p)})
    def work_one(self, wait):
        got = self.r.xreadgroup("workers", self.name, {"jobs": ">"}, count=1, block=500 if wait else None)
        if not got:
            return None
        mid, fields = got[0][1][0]
        p = json.loads(fields[b"p"])
        self.r.xack("jobs", "workers", mid)
        return p

BACKENDS = {"pg": PG, "redis_list": RedisList, "redis_stream": RedisStream}

# ------------------------------------------------------------------ workloads
def producer(backend, tier, n, start, barrier):
    b = BACKENDS[backend](tier); barrier.wait()
    for i in range(start, start + n):
        b.enqueue(payload(i))

def drainer(backend, tier, barrier, out):
    b = BACKENDS[backend](tier); barrier.wait(); n = 0
    while b.work_one(wait=False) is not None:
        n += 1
    out.put(n)

def latency_worker(backend, tier, stop_at, out):
    b = BACKENDS[backend](tier); lats = []
    while time.time() < stop_at:
        p = b.work_one(wait=True)
        if p is not None:
            lats.append(time.time() - p["enqueued_at"])
    out.put(lats)

def run(backend, tier, test, param):
    BACKENDS[backend].reset(tier)
    ctx = mp.get_context("spawn")
    n_jobs = N_STRICT if tier.startswith("strict") else N_FAST
    if test == "enqueue":
        per = n_jobs // param; bar = ctx.Barrier(param + 1)
        ps = [ctx.Process(target=producer, args=(backend, tier, per, k * per, bar)) for k in range(param)]
        [p.start() for p in ps]; bar.wait(); t0 = time.perf_counter(); [p.join() for p in ps]
        dt = time.perf_counter() - t0
        return {"jobs": per * param, "seconds": round(dt, 3), "jobs_per_s": round(per * param / dt)}
    if test == "drain":
        b = BACKENDS[backend]("relaxed")
        for i in range(n_jobs):                      # preload (not timed)
            b.enqueue(payload(i))
        bar = ctx.Barrier(param + 1); q = ctx.Queue()
        ws = [ctx.Process(target=drainer, args=(backend, tier, bar, q)) for _ in range(param)]
        [w.start() for w in ws]; bar.wait(); t0 = time.perf_counter(); [w.join() for w in ws]
        dt = time.perf_counter() - t0; done = sum(q.get() for _ in ws)
        return {"jobs": done, "seconds": round(dt, 3), "jobs_per_s": round(done / dt), "all_drained": done == n_jobs}
    if test == "latency":
        stop_at = time.time() + SECONDS + 3; q = ctx.Queue()
        ws = [ctx.Process(target=latency_worker, args=(backend, tier, stop_at, q)) for _ in range(param)]
        [w.start() for w in ws]; time.sleep(2)       # let workers connect and start waiting
        b = BACKENDS[backend](tier); n = RATE * SECONDS; t0 = time.perf_counter()
        for i in range(n):
            target = t0 + i / RATE
            while time.perf_counter() < target:
                pass
            b.enqueue(payload(i))
        lats = sorted(x for _ in ws for x in q.get()); [w.join() for w in ws]
        pct = lambda p: round(1000 * lats[min(len(lats) - 1, int(len(lats) * p))], 2)
        return {"jobs": len(lats), "sent": n, "p50_ms": pct(.5), "p99_ms": pct(.99), "max_ms": round(1000 * lats[-1], 2)}

if __name__ == "__main__":
    backend, tier, test, param, rep = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
    extra = {}
    if backend == "pg":
        import psycopg
        with psycopg.connect("host=localhost port=5499 user=bench dbname=postgres") as c:
            extra["wal_sync_method"] = c.execute("show wal_sync_method").fetchone()[0]
    print(json.dumps({"backend": backend, "tier": tier, "test": test, "param": param, "rep": rep, **extra, **run(backend, tier, test, param)}))
