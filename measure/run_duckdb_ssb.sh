#!/bin/bash

# execute queries
dir="$SSB_PATH/ssb/queries"
iteration=1

rm -rf ssb_result/
mkdir -p ssb_result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a duckdb_query_split_ssb_${i}.txt;
    echo -ne ".read ${sql}" | duckdb ./ssb.duckdb 2>&1|tee -a duckdb_query_split_ssb_${i}.txt;
  done
done

mv duckdb_query_split_ssb_* ssb_result/.
