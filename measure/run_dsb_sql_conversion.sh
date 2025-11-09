#!/bin/bash

if [ -z "$1" ]; then
  echo "Please enter scale factor to choose the correct database!"
  exit 1
fi

# execute queries
dir_1="$DSB_PATH/code/tools/1_instance_out_wo_multi_block/1/"
dir_2="$DSB_PATH/code/tools/1_instance_out_wo_multi_block/2/"
iteration=1

log_name=duckdb_dsb_$1_sql_conversion_result.txt

rm -rf ${log_name}
rm -rf dsb_$1_result/${log_name}
mkdir -p dsb_$1_result/

# compile
echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_CONVERT_DUCKDB_TO_IR=1 ENABLE_CONVERT_IR_TO_SQL=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure

for i in $(eval echo {1.."${iteration}"}); do
  for sql in $(find "$dir_1" "$dir_2" -type f -name "*.sql"); do
    rm -f dd_sub_plan_*
    echo "execute ${sql}" 2>&1|tee -a ${log_name};
    echo -ne ".read ${sql}" | duckdb ./dsb_$1.db 2>&1|tee -a ${log_name};

#  # count the number of files with pattern `dd_sub_plan_*`
#  count=$(find "${PWD}" -maxdepth 1 -type f -name 'dd_sub_plan_*.sql' | wc -l)

#  # skip if count is 1 and run the last sql separately
#  if [ "$count" -gt 1 ]; then
#    for i in $(seq 1 $((count - 1))); do
#      duckdb -c ".read ${PWD}/dd_sub_plan_${i}.sql" imdb.db
#    done
#  fi
#
#  # run last file separately
#  echo "execute ${sql}" 2>&1|tee -a ${log_name};
#  echo -ne ".read ${PWD}/dd_sub_plan_${count}.sql" | duckdb ./imdb.db 2>&1 | tee -a "${log_name}"

#  # drop temp tables for counts > 1
#  if [ "${count}" -gt 1 ]; then
#    for i in $(seq 1 $((count - 1))); do
#      duckdb -c "drop table temp${i};" imdb.db
#    done
#  fi
  done
done

mv ${log_name} dsb_$1_result/.

