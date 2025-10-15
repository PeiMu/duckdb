#!/bin/bash

if [ -z "$1" ]; then
  echo "Please enter Official or QuerySplit!"
  exit 1
fi

if [ -z "$2" ]; then
  echo "Please enter scale factor to choose the correct database!"
  exit 1
fi

# execute queries
dir_1="$DSB_PATH/code/tools/1_instance_out_wo_multi_block/1/"
dir_2="$DSB_PATH/code/tools/1_instance_out_wo_multi_block/2/"
iteration=1

log_name=duckdb_result_dsb_$2_$1.txt

rm -rf ${log_name}
rm -rf dsb_$2_result/${log_name}
mkdir -p dsb_$2_result/

for i in $(eval echo {1.."${iteration}"}); do
  for sql in $(find "$dir_1" "$dir_2" -type f -name "*.sql"); do
    echo "execute ${sql}" 2>&1|tee -a ${log_name};
    echo -ne ".read ${sql}" | duckdb /home/pei/Project/duckdb/measure/dsb_$2.db 2>&1|tee -a ${log_name};
  done
done

mv ${log_name} dsb_$2_result/.

