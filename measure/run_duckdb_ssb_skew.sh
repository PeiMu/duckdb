#!/bin/bash

# execute queries
dir="$SSB_PATH/ssb-skew/queries/"
iteration=1

rm -rf ssb_skew_result/
mkdir -p ssb_skew_result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a duckdb_query_split_ssb_skew_${i}.txt;
    echo -ne ".read ${sql}" | duckdb ./ssb_skew.duckdb 2>&1|tee -a duckdb_query_split_ssb_skew_${i}.txt;
  done
done

mv duckdb_query_split_ssb_skew_* ssb_skew_result/.
