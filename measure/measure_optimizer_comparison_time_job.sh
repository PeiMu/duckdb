#!/bin/bash

if [ -z "$1" ]; then
  echo "Please enter whole_query or query_split!"
  exit 1
fi
if [ -z "$2" ]; then
  echo "Please enter DuckDB version, e.g., 0.6.1, 0.10.1, 1.3.2, etc.!"
  exit 1
fi

# execute queries
dir="$JOB_PATH/queries_support_converter"
iteration=15 # 5 warm up, 10 runs

LOG_NAME=optimizer_comparison_time_log.csv

rm -rf *${LOG_NAME}
rm -rf job_result/$1/duckdb_$2_${LOG_NAME}


cd ../../IR_SQL_Converter/build_duckdb_132/ && make clean && make -j32 && cd ../../duckdb_132/measure/
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make >> compile.log 2>&1 && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  filename=${sql%/}        # remove trailing /
  filename=${filename##*/} # remove everything before last /
  id=${filename%.sql}      # remove .sql
  echo "sql id is: ${id}"
  if [[ ! -d "${PWD}/job_result/$1/$2/${id}/" ]]; then
    echo "${PWD}/job_result/$1/$2/${id}/ doesn't exists!!!"
    exit 1
  fi
  for i in $(eval echo {1.."${iteration}"}); do
    ../build/release/optimizer_comparison ${PWD}/imdb.db ${PWD}/job_result/$1/$2/${id}/
  done
done
mv ${LOG_NAME} duckdb_$2_${LOG_NAME}

mv duckdb_$2_${LOG_NAME} job_result/$1/.
