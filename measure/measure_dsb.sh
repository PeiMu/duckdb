#!/bin/bash

if [ -z "$1" ]; then
  echo "Please enter scale factor to choose the correct database!"
  exit 1
fi

mkdir -p dsb_$1_result/
rm -rf compile.log

echo "official" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./hyperfine_in_mem_dsb.sh official nan $1

############################# estimated stats #############################
#echo "query_split with join_order_optimization before query_split, with estimated stats" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem_dsb.sh query_split js_wo_stats_stats $1

echo "query_split with join_order_optimization after query_split, with estimated stats" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./hyperfine_in_mem_dsb.sh query_split rsj_wo_stats_stats $1
############################# estimated stats #############################

#echo "query_split with join_order_optimization before query_split" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem_dsb.sh query_split js $1

echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./hyperfine_in_mem_dsb.sh query_split rsj $1

mv compile.log dsb_$1_result/.
