#!/bin/bash

# execute queries
dir="$JOB_COMPLEX_PATH/sql_queries"
iteration=1

log_name=duckdb_job_complex_result_$1.txt

rm -rf ${log_name}
rm -rf job_complex_result/${log_name}
mkdir -p job_complex_result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a ${log_name};
    echo -ne ".read ${sql}" | duckdb ./imdb.db 2>&1|tee -a ${log_name};
  done
done

mv ${log_name} job_complex_result/.
