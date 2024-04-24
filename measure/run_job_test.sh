#!/bin/bash

DB_NAME=imdb_test.db

rm -f ./${DB_NAME}

# create schema
echo "create imdb schema"
echo -ne ".read imdb_test_schema.sql" | duckdb ./${DB_NAME}

# load imdb
for table in name title cast_info keyword movie_keyword
do
  echo "duckdb load table from ${table}.tbl"
  command="copy ${table} from '/home/pei/Project/benchmarks/imdb_job-postgres/test_csv/${table}.csv' (quote '\"', escape '\\');"
  #command="copy ${table} from '/home/pei/Project/benchmarks/imdb_job-postgres/csv/${table}.csv' (quote '\"', escape '\\');"
  echo $command
  echo -ne "${command}" | duckdb ./${DB_NAME}
done

## execute queries
#dir="/home/pei/Project/benchmarks/imdb_job-postgres/skinner_explained"
#iteration=10
#
#rm -rf job_result/
#mkdir -p job_result/
#
#for i in $(eval echo {1.."${iteration}"}); do
#  for sql in "${dir}"/*; do
#    echo "execute ${sql}" 2>&1|tee -a skinner_explained_imdb_${i}.txt;
#    echo -ne ".read ${sql}" | duckdb ./imdb.db 2>&1|tee -a skinner_explained_imdb_${i}.txt;
#  done
#done
#
#mv skinner_explained_imdb_* job_result/.
