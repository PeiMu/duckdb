#!/bin/bash

log_name=duckdb_$1_$2_${i}.txt

rm -rf duckdb_$1_$2_*
rm -rf job_result/duckdb_$1_$2_*

dir="/home/pei/Project/benchmarks/imdb_job-postgres/queries"
iteration=10

for sql in "${dir}"/*.sql; do
  echo "hyperfine run ${sql}" 2>&1|tee -a ${log_name}
  hyperfine --warmup 3 --runs ${iteration} "duckdb -c \".read ${sql}\" ./imdb.db" 2>&1|tee -a ${log_name}
done

mv duckdb_$1_$2_* job_result/.
