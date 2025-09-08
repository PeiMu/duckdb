#!/bin/bash

log_name=duckdb_$1_$2.csv

rm -rf ${log_name}
rm -rf job_result/${log_name}

dir="$JOB_PATH/queries"
iteration=10

for sql in "${dir}"/*.sql; do
#  echo "hyperfine run ${sql}" 2>&1|tee -a ${log_name}
  hyperfine --warmup 5 --runs ${iteration} --export-csv temp.csv "duckdb -c \".read ${sql}\" ./imdb.db"
  cat temp.csv >> ${log_name}
done

mv ${log_name} job_result/.
rm temp.csv
