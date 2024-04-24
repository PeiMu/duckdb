#!/bin/bash

rm -f ./test.db

# create schema
echo "create test schema"
echo -ne ".read test_schema.sql" | duckdb ./test.db

# load imdb
for table in name title cast_info keyword movie_keyword 
do
  echo "duckdb load table from ${table}.tbl"
  command="copy ${table} from '/home/pei/Project/benchmarks/imdb_job-postgres/csv/${table}.csv' (quote '\"', escape '\\');"
  echo $command
  echo -ne "${command}" | duckdb ./test.db
done

