#!/bin/bash

$rm_pg_log
$pg_start

rm -f convert_postgres.log

dir="/home/pei/Project/benchmarks/imdb_job-postgres/queries_without_AS"
iteration=1

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "convert ${sql}" 2>&1|tee -a convert_postgres.log;
    psql -U imdb -d imdb -h /tmp -f "${sql}" 2>&1|tee -a convert_postgres.log;
    duckdb -c ".read ${sql}" imdb.db 2>&1|tee -a convert_postgres.log;
  done
done

$pg_stop
