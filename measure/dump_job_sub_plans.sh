#!/bin/bash

# execute queries
dir="$JOB_PATH/queries"
iteration=1

log_name=duckdb_job_result_$1.txt

rm -rf ${log_name}
rm -rf job_result/${log_name}
mkdir -p job_result/

# change `ENABLE_OPTIMIZER_COMPARISON` to true
sed -i 's/#define ENABLE_OPTIMIZER_COMPARISON\s\+false/#define ENABLE_OPTIMIZER_COMPARISON true/' ../src/include/duckdb/optimizer/query_split/query_split.hpp
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make && cd measure/
# rest
sed -i 's/#define ENABLE_OPTIMIZER_COMPARISON\s\+true/#define ENABLE_OPTIMIZER_COMPARISON false/' ../src/include/duckdb/optimizer/query_split/query_split.hpp

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    filename=${sql%/}        # remove trailing /
    filename=${filename##*/} # remove everything before last /
    id=${filename%.sql}      # remove .sql
    rm -rf ${PWD}/job_result/${id}/
  done
done

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a ${log_name};
    echo -ne ".read ${sql}" | duckdb ./imdb.db 2>&1|tee -a ${log_name};
    filename=${sql%/}        # remove trailing /
    filename=${filename##*/} # remove everything before last /
    id=${filename%.sql}      # remove .sql
    echo "sql id is: ${id}"
    mkdir ${PWD}/job_result/${id}/
    mv *.bin ${PWD}/job_result/${id}/
  done
done

mv ${log_name} job_result/.
