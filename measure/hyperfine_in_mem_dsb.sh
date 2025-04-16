#!/bin/bash

log_name=duckdb_$1_$2.csv

rm -rf duckdb_$1_$2.csv
rm -rf dsb_result/duckdb_$1_$2.csv

dir_1="/home/pei/Project/benchmarks/dsb-postgres/code/tools/1_instance_out_wo_multi_block/1/"
dir_2="/home/pei/Project/benchmarks/dsb-postgres/code/tools/1_instance_out_wo_multi_block/2/"
iteration=10

for sql in $(find "$dir_1" "$dir_2" -type f -name "*.sql"); do
#  echo "hyperfine run ${sql}" 2>&1|tee -a ${log_name}
  hyperfine --warmup 5 --runs ${iteration} --export-csv temp.csv "duckdb -c \".read ${sql}\" ./dsb.db"
  cat temp.csv >> ${log_name}
done

mv duckdb_$1_$2_dsb.csv dsb_result/.
rm temp.csv
