#!/bin/bash

if [ -z "$1" ]; then
  echo "Please enter scale factor to choose the correct database!"
  exit 1
fi

echo "official" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh official $1

echo "query_split with join_order_optimization before query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh js $1

#echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh rsj $1

diff dsb_$1_result/duckdb_result_dsb_$1_official.txt dsb_$1_result/duckdb_result_dsb_$1_js.txt 2>&1 | tee dsb_$1_js_diff.log
#diff dsb_$1_result/duckdb_result_dsb_$1_official.txt dsb_$1_result/duckdb_result_dsb_$1_rsj.txt 2>&1 | tee dsb_$1_rsj_diff.log

echo "query_split with join_order_optimization before query_split and with merging back to the whole plan" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh js_merge_back $1

#echo "query_split with join_order_optimization after query_split and with merging back to the whole plan" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh rsj_merge_back $1

diff dsb_$1_result/duckdb_result_dsb_$1_official.txt dsb_$1_result/duckdb_result_dsb_$1_js_merge_back.txt 2>&1 | tee dsb_$1_js_merge_back_diff.log
#diff dsb_$1_result/duckdb_result_dsb_$1_official.txt dsb_$1_result/duckdb_result_dsb_$1_rsj_merge_back.txt 2>&1 | tee dsb_$1_rsj_merge_back_diff.log

echo "query_split with join_order_optimization before query_split with specified stat" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh js_stat $1

#echo "query_split with join_order_optimization after query_split with specified stat" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh rsj_stat $1

echo "query_split with join_order_optimization before query_split with specified stat and with merging back to the whole plan" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh js_stat_merge_back $1

#echo "query_split with join_order_optimization after query_split with specified stat and with merging back to the whole plan" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure && bash ./run_duckdb_dsb.sh rsj_stat_merge_back $1