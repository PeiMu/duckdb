#!/bin/bash

rm -f ./tpch.db

# create schema
echo "create tpch schema"
echo -ne ".read tpch_schema.sql" | duckdb ./tpch.db

# load tpch
for table in region nation part supplier partsupp customer orders lineitem
do
  echo "duckdb load table from ${table}.tbl"
  command="copy ${table} from '/home/pei/Project/benchmarks/tpch-postgres/dbgen/out/${table}.tbl';"
  echo $command
  echo -ne "${command}" | duckdb ./tpch.db
done

# execute queries
dir="/home/pei/Project/benchmarks/tpch-postgres/dbgen/out/skinner_explained"
iteration=10

rm -rf tpch_result/
mkdir -p tpch_result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a skinner_explained_tpch-1_${i}.txt;
    echo -ne ".read ${sql}" | duckdb ./tpch.db 2>&1|tee -a skinner_explained_tpch-1_${i}.txt;
  done
done

mv skinner_explained_tpch-1_* tpch_result/.
