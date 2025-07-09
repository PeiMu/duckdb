#!/bin/bash

if [ -z "$1" ]; then
  echo "Please enter Official or QuerySplit!"
  exit 1
fi

if [ -z "$2" ]; then
  echo "Please enter scale factor to choose the correct database!"
  exit 1
fi

#rm -f ./dsb_$2.db
#
#cd ../ && make clean && GEN=ninja VERBOSE=1 make 2>&1|tee -a compile.log && cd measure
#
## create schema
#echo "create dsb schema"
#echo -ne ".read create_tables.sql" | duckdb ./dsb_$2.db
#
## load dsb
#for table in customer_address customer_demographics date_dim warehouse ship_mode time_dim reason income_band item store call_center customer web_site store_returns household_demographics web_page promotion catalog_page inventory catalog_returns web_returns web_sales catalog_sales store_sales 
#do
#  echo "duckdb load table from ${table}.tbl"
#  if [ "$2" -eq 10 ]; then
#    command="copy ${table} from '/home/pei/Project/benchmarks/dsb-postgres/code/tools/out/csv/${table}.csv' (quote '\"', escape '\\');"
#  elif [ "$2" -eq 100 ]; then
#    command="copy ${table} from '/home/pei/Project/benchmarks/dsb-postgres/code/tools/out_100/csv/${table}.csv' (quote '\"', escape '\\');"
#  else
#    echo "Please enter a correct scale factor 10/100, or check the csv file path!"
#  fi
#  echo $command
#  echo -ne "${command}" | duckdb ./dsb_$2.db
#done


# execute queries
dir_1="/home/pei/Project/benchmarks/dsb-postgres/code/tools/1_instance_out_wo_multi_block/1/"
dir_2="/home/pei/Project/benchmarks/dsb-postgres/code/tools/1_instance_out_wo_multi_block/2/"
iteration=1

log_name=duckdb_result_dsb_$2_$1.txt

rm -rf ${log_name}
rm -rf dsb_$2_result/${log_name}
mkdir -p dsb_$2_result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in $(find "$dir_1" "$dir_2" -type f -name "*.sql"); do
    echo "execute ${sql}" 2>&1|tee -a ${log_name};
    echo -ne ".read ${sql}" | duckdb ./dsb_$2.db 2>&1|tee -a ${log_name};
  done
done

mv ${log_name} dsb_$2_result/.

