#!/bin/bash

rm -f ./tpch.db

# create schema
echo "create tpch schema"
echo -ne ".read tpch_schema.sql" | duckdb ./tpch.db

# load tpch
for table in region nation part supplier partsupp customer orders lineitem
do
  echo "duckdb load table from ${table}.tbl"
  command="insert into ${table} select * from read_csv('/home/pei/Project/benchmarks/tpch-postgres/dbgen/out/${table}.tbl');"
  echo $command
  echo -ne "${command}" | duckdb ./tpch.db
done

