#!/bin/bash

rm -rf job_result/
mkdir -p job_result/
rm -rf compile.log

echo "official" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem.sh official nan

if [ $1 == 'estimated_stats' ]
then
############################# estimated stats #############################
echo "query_split with join_order_optimization before query_split, with estimated stats" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem.sh query_split jop_before_estimated_stats

echo "query_split with join_order_optimization after query_split, with estimated stats" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem.sh query_split jop_after_estimated_stats
############################# estimated stats #############################
fi

echo "query_split with join_order_optimization before query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem.sh query_split jop_before

echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem.sh query_split jop_after

mv compile.log job_result/.
