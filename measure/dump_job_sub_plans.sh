#!/bin/bash

# execute queries
dir="$JOB_PATH/queries_support_converter"
iteration=1

log_name=duckdb_job_result_$1.txt

rm -rf ${log_name}
rm -rf job_result/${log_name}
mkdir -p job_result/
mkdir -p job_result/1.3.2/

cd ../../IR_SQL_Converter/build_duckdb_132/ && make clean && make -j32 && cd ../../duckdb_132/measure/
# change `ENABLE_SERIALIZE_IR` to true
sed -i 's/#define ENABLE_SERIALIZE_IR\s\+false/#define ENABLE_SERIALIZE_IR true/' ${PWD}/../src/include/duckdb/optimizer/query_split/query_split.hpp
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make >> compile.log 2>&1 && cd measure/
# rest
sed -i 's/#define ENABLE_SERIALIZE_IR\s\+true/#define ENABLE_SERIALIZE_IR false/' ${PWD}/../src/include/duckdb/optimizer/query_split/query_split.hpp

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    filename=${sql%/}        # remove trailing /
    filename=${filename##*/} # remove everything before last /
    id=${filename%.sql}      # remove .sql
    rm -rf ${PWD}/job_result/1.3.2/${id}/
  done
done

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a ${log_name};
    echo -ne ".read ${sql}" | ../build/release/duckdb ./imdb.db 2>&1|tee -a ${log_name};
    filename=${sql%/}        # remove trailing /
    filename=${filename##*/} # remove everything before last /
    id=${filename%.sql}      # remove .sql
    echo "sql id is: ${id}"
    mkdir ${PWD}/job_result/1.3.2/${id}/
    mv *.ir ${PWD}/job_result/1.3.2/${id}/
  done
done

mv ${log_name} job_result/.
