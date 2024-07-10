#!/bin/bash

rm -rf job_result/
mkdir -p job_result/
rm -rf compile.log

echo "official" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem.sh official nan

echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem.sh query_split jop_after

echo "query_split with join_order_optimization before query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem.sh query_split jop_before

mv compile.log job_result/.
