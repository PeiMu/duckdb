#!/bin/bash

dir="$JOB_PATH/queries"
iteration=10

mkdir -p convert_job_sql/
rm -rf compile.log

log_name=duckdb_job_performance.csv

rm -rf ${log_name}
rm -rf convert_job_sql/${log_name}

# compile
echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_CONVERT_DUCKDB_TO_IR=1 ENABLE_CONVERT_IR_TO_SQL=1" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_CONVERT_DUCKDB_TO_IR=1 ENABLE_CONVERT_IR_TO_SQL=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure/

for sql in "${dir}"/*.sql; do
  rm -f dd_sub_plan_*
  hyperfine --warmup 5 --runs ${iteration} --export-csv temp.csv --conclude "for i in \$(seq 1 \$((\$(find \${PWD} -maxdepth 1 -type f -name \"dd_sub_plan_*.sql\" | wc -l)-1))); do duckdb -c \"drop table temp\${i};\" imdb.db; done;" "duckdb -c \".read ${sql}\" ./imdb.db"
  cat temp.csv >> ${log_name}
done

mv ${log_name} convert_job_sql/.
rm temp.csv

