#!/bin/bash

if [ -z "$1" ]; then
  echo "Please enter Official or QuerySplit!"
  exit 1
fi

if [ -z "$2" ]; then
  echo "Please enter scale factor to choose the correct database!"
  exit 1
fi

rm -f ./tpcds_$2.db

cd ../ && make clean && GEN=ninja VERBOSE=1 make 2>&1|tee -a compile.log && cd measure

# create schema
echo "create tpcds schema"
echo -ne ".read create_tables.sql" | duckdb ./tpcds_$2.db

# load tpcds
for table in customer_address customer_demographics date_dim warehouse ship_mode time_dim reason income_band item store call_center customer web_site store_returns household_demographics web_page promotion catalog_page inventory catalog_returns web_returns web_sales catalog_sales store_sales 
do
  echo "duckdb load table from ${table}.tbl"
  if [ "$2" -eq 1 ]; then
    command="copy ${table} from '$TPCDS_PATH/tools/out_1/csv/${table}.csv' (quote '\"', escape '\\');"
  elif [ "$2" -eq 2 ]; then
    command="copy ${table} from '$TPCDS_PATH/tools/out_2/csv/${table}.csv' (quote '\"', escape '\\');"
  else
    echo "Please enter a correct scale factor 10/100, or check the csv file path!"
  fi
  echo $command
  echo -ne "${command}" | duckdb ./tpcds_$2.db
done
