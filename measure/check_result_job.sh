#!/bin/bash

echo "official" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./run_duckdb_job.sh official

#echo "query_split with join_order_optimization before query_split" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./run_duckdb_job.sh

echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./run_duckdb_job.sh rsj

diff job_result/duckdb_job_result_official.txt job_result/duckdb_job_result_rsj.txt 2>&1 | tee job_rsj_diff.log

#echo "query_split with join_order_optimization after query_split and with merging back to the whole plan" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./run_duckdb_job.sh rsj_merge_back

#diff job_result/duckdb_job_result_official.txt job_result/duckdb_job_result_rsj_merge_back.txt 2>&1 | tee job_rsj_merge_back_diff.log
