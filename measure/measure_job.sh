#!/bin/bash

rm -rf job_result/
mkdir -p job_result/
rm -rf compile.log

echo "official" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem_job.sh official nan

# without updating statistics
if [ $1 == 'estimated_stats' ]
then
############################# estimated stats #############################
#echo "query_split with join_order_optimization before query_split, with estimated stats" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem_job.sh query_split js_wo_stats_stats

echo "query_split with join_order_optimization after query_split, with estimated stats" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem_job.sh query_split rsj_wo_stats_stats
############################# estimated stats #############################
fi

#echo "query_split with join_order_optimization before query_split" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem_job.sh query_split js

echo "query_split with join_order_optimization after query_split" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make 2>&1|tee -a compile.log && cd measure && bash ./hyperfine_in_mem_job.sh query_split rsj

mv compile.log job_result/.
