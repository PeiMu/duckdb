#!/bin/bash

log_name=breakdown_duckdb_$1_$2_${i}.txt

rm -rf breakdown_duckdb_$1_$2_*
rm -rf break_down_$3/breakdown_duckdb_$1_$2_*

echo -ne ".read /home/pei/Project/benchmarks/imdb_job-postgres/skinnerdb_queries/$3.sql" | duckdb ./imdb.db
{ time echo -ne ".read /home/pei/Project/benchmarks/imdb_job-postgres/skinnerdb_queries/$3.sql" | duckdb ./imdb.db ; } 2>&1|tee -a  ${log_name}

mv breakdown_duckdb_$1_$2_* break_down_$3/.
