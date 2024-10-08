#!/bin/bash

$rm_pg_log
$pg_start

rm -f convert_postgres.log

clear && cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && clear

dir="/home/pei/Project/benchmarks/imdb_job-postgres/queries_without_AS_QuerySplit"
iteration=1

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "convert ${sql}" 2>&1|tee -a convert_postgres.log;
    psql -U imdb -d imdb -h /tmp -f "${sql}" 2>&1|tee -a convert_postgres.log;
    duckdb -c ".read ${sql}" imdb.db 2>&1|tee -a convert_postgres.log;
  done
done

$pg_stop
