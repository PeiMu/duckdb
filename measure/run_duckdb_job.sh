#!/bin/bash

rm -f ./imdb.db

# create schema
echo "create imdb schema"
echo -ne ".read imdb_schema.sql" | duckdb ./imdb.db

# load imdb
for table in name aka_name kind_type title aka_title char_name role_type cast_info comp_cast_type company_name company_type complete_cast info_type keyword link_type movie_companies movie_info movie_info_idx movie_keyword movie_link person_info
do
  echo "duckdb load table from ${table}.tbl"
  command="copy ${table} from '/home/pei/Project/benchmarks/imdb_job-postgres/csv/${table}.csv' (quote '\"', escape '\\');"
  echo $command
  echo -ne "${command}" | duckdb ./imdb.db
done

## execute queries
#dir="/home/pei/Project/benchmarks/imdb_job-postgres/skinnerdb_queries"
#iteration=1
#
#rm -rf job_result/
#mkdir -p job_result/
#
#for i in $(eval echo {1.."${iteration}"}); do
#  for sql in "${dir}"/*; do
#    echo "execute ${sql}" 2>&1|tee -a duckdb_query_split_imdb_${i}.txt;
#    echo -ne ".read ${sql}" | duckdb ./imdb.db 2>&1|tee -a duckdb_query_split_imdb_${i}.txt;
#  done
#done
#
#mv duckdb_query_split_imdb_* job_result/.
