#!/bin/bash

# execute queries
dir="$JOB_PATH/queries"
iteration=15 # 5 warm up, 10 runs

LOG_NAME=time_log.csv

rm -rf *${LOG_NAME}
rm -rf convert_job_sql/*${LOG_NAME}


###### compile
echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_CONVERT_DUCKDB_TO_IR=1 ENABLE_CONVERT_IR_TO_SQL=1 ENABLE_MEASURE_EXE_TIME=1" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_CONVERT_DUCKDB_TO_IR=1 ENABLE_CONVERT_IR_TO_SQL=1 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make >> compile.log 2>&1 && cd measure/
for sql in "${dir}"/*.sql; do
  rm -f dd_sub_plan_*
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;

    # count the number of files with pattern `dd_sub_plan_*`
    count=$(find "${PWD}" -maxdepth 1 -type f -name 'dd_sub_plan_*.sql' | wc -l)
    # drop temp tables for counts > 1
    if [ "${count}" -gt 1 ]; then
      for j in $(seq 1 $((count - 1))); do
        duckdb -c "drop table temp${j};" imdb.db
      done
    fi

  done
done
mv ${LOG_NAME} duckdb_sql_conversion_breakdown_${LOG_NAME}


mv *${LOG_NAME} convert_job_sql/.
