#!/bin/bash

dir="/home/pei/Project/benchmarks/imdb_job-postgres/queries_without_AS"

mkdir -p convert_job_sql/
rm -rf compile.log

log_name=duckdb_job_result.txt

rm -rf ${log_name}
rm -rf convert_job_sql/${log_name}

# compile
echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_CONVERT_DUCKDB_TO_IR=1 ENABLE_CONVERT_IR_TO_SQL=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure


#duckdb -c ".read /home/pei/Project/benchmarks/imdb_job-postgres/queries_without_AS/6d.sql" imdb.db

for sql in "${dir}"/*.sql; do
  rm -f dd_sub_plan_*

#  duckdb -c ".read ${sql}" imdb.db
  echo "execute ${sql}" 2>&1|tee -a ${log_name};
  echo -ne ".read ${sql}" | duckdb ./imdb.db 2>&1 | tee -a "${log_name}"

  # count the number of files with pattern `dd_sub_plan_*`
  count=$(find "${PWD}" -maxdepth 1 -type f -name 'dd_sub_plan_*.sql' | wc -l)

#  # skip if count is 1 and run the last sql separately
#  if [ "$count" -gt 1 ]; then
#    for i in $(seq 1 $((count - 1))); do
#      duckdb -c ".read ${PWD}/dd_sub_plan_${i}.sql" imdb.db
#    done
#  fi
#
#  # run last file separately
#  echo "execute ${sql}" 2>&1|tee -a ${log_name};
#  echo -ne ".read ${PWD}/dd_sub_plan_${count}.sql" | duckdb ./imdb.db 2>&1 | tee -a "${log_name}"

  # drop temp tables for counts > 1
  if [ "${count}" -gt 1 ]; then
    for i in $(seq 1 $((count - 1))); do
      duckdb -c "drop table temp${i};" imdb.db
    done
  fi
done


mv ${log_name} convert_job_sql/.

