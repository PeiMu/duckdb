#!/bin/bash

log_name=hyperfine_convert_postgres_plan.txt

rm -rf ${log_name}
rm -rf job_result/${log_name}

clear && cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && clear

#dir="$JOB_PATH/queries_without_AS"
dir="$JOB_PATH/QuerySplit/queries_new_settings"
iteration=10

for sql in "${dir}"/*.sql; do
  echo "convert ${sql}" 2>&1|tee -a convert_postgres.log;
  psql -U imdb -d imdb -f "${sql}" 2>&1|tee -a convert_postgres.log;
  echo "hyperfine run ${sql}" 2>&1|tee -a ${log_name}
  hyperfine --warmup 5 --runs ${iteration} -i "duckdb -c \".read ${sql}\" ./imdb.db" 2>&1|tee -a ${log_name}
done

mv ${log_name} job_result/
