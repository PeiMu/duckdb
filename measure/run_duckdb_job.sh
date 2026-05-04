#!/bin/bash

# execute queries
dir="$JOB_PATH/queries"
iteration=1

log_name=duckdb_job_result_$1.txt

rm -rf ${log_name}
rm -rf job_result/${log_name}
mkdir -p job_result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a ${log_name};
    echo -ne ".read ${sql}" | duckdb ./imdb.db 2>&1|tee -a ${log_name};
  done
done

mv ${log_name} job_result/.
