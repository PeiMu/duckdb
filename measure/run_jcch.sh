#!/bin/bash

rm -f ./jcch.db

# create schema
echo "create jcch schema"
echo -ne ".read jcch_schema.sql" | duckdb ./jcch.db

# load jcch
for table in region nation part supplier partsupp customer orders lineitem
do
  echo "duckdb load table from ${table}.tbl"
  command="copy ${table} from '/home/pei/Project/benchmarks/JCC-H/out/${table}.tbl';"
  #command="insert into ${table} select * from read_csv('/home/pei/Project/benchmarks/JCC-H/out/${table}.tbl');"
  echo $command
  echo -ne "${command}" | duckdb ./jcch.db
done

# execute queries
dir="/home/pei/Project/benchmarks/JCC-H/out/skinner_explained"
iteration=10

rm -rf jcch_result/
mkdir -p jcch_result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in "${dir}"/*; do
    echo "execute ${sql}" 2>&1|tee -a skinner_explained_jcch-1_${i}.txt;
    echo -ne ".read ${sql}" | duckdb ./jcch.db 2>&1|tee -a skinner_explained_jcch-1_${i}.txt;
  done
done

mv skinner_explained_jcch-1_* jcch_result/.
