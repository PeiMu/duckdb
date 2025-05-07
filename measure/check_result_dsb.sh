#!/bin/bash

echo "official" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./run_duckdb_dsb.sh official

#echo "query_split with join_order_optimization before query_split" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./run_duckdb_job.sh

echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./run_duckdb_dsb.sh rsj

diff dsb_result/duckdb_result_dsb_official.txt dsb_result/duckdb_result_dsb_rsj.txt 2>&1 | tee dsb_rsj_diff.log

echo "query_split with join_order_optimization after query_split and with merging back to the whole plan" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./run_duckdb_dsb.sh rsj_merge_back

diff dsb_result/duckdb_result_dsb_official.txt dsb_result/duckdb_result_dsb_rsj_merge_back.txt 2>&1 | tee dsb_rsj_merge_back_diff.log
