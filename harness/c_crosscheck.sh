#!/bin/bash
# The two C-client cross-checks from the article (relaxed durability, one and four connections).
redis-cli -p 6399 config set appendonly yes >/dev/null; redis-cli -p 6399 config set appendfsync everysec >/dev/null
redis-benchmark -p 6399 -c 1 -n 100000 -d 200 -t lpush --csv
redis-benchmark -p 6399 -c 4 -n 200000 -d 200 -t lpush --csv
psql -h localhost -p 5499 -U bench -d queue -qc "drop table if exists jobs; create table jobs (id bigserial primary key, payload jsonb not null)"
pgbench -h localhost -p 5499 -U bench -n -c 1 -T 10 -M prepared -f "$(dirname "$0")/pgbench_insert.sql" queue
pgbench -h localhost -p 5499 -U bench -n -c 4 -j 4 -T 10 -M prepared -f "$(dirname "$0")/pgbench_insert.sql" queue
