#!/bin/bash

# execute queries
dir="$JOB_PATH/queries"
iteration=15 # 5 warm up, 10 runs

LOG_NAME=time_log.csv

rm -rf *${LOG_NAME}
rm -rf job_result/*${LOG_NAME}


###### official duckdb
echo "compile official duckdb" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make >> compile.log 2>&1 && cd measure/
echo "PreOptimize, final-PostOptimize, final-CreatePlan, Execute"  >> ${LOG_NAME};
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} duckdb_official_breakdown_${LOG_NAME}


###### without updating statistics
echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MEASURE_EXE_TIME=1" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MEASURE_EXE_TIME=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} duckdb_js_wo_stats_breakdown_${LOG_NAME}

#echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MEASURE_EXE_TIME=1" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MEASURE_EXE_TIME=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure/
#for sql in "${dir}"/*; do
#  echo "execute ${sql}" >> ${LOG_NAME};
#  for i in $(eval echo {1.."${iteration}"}); do
#    echo -ne ".read ${sql}" | duckdb ./imdb.db;
#  done
#done
#mv ${LOG_NAME} duckdb_rsj_wo_stats_breakdown_${LOG_NAME}

###### join order opt + split + merge back to the whole plan
echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} duckdb_js_whole_plan_wo_stats_breakdown_${LOG_NAME}

####### reorder table + split + join order opt + merge back to the whole plan
#echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure/
#for sql in "${dir}"/*; do
#  echo "execute ${sql}" >> ${LOG_NAME};
#  for i in $(eval echo {1.."${iteration}"}); do
#    echo -ne ".read ${sql}" | duckdb ./imdb.db;
#  done
#done
#mv ${LOG_NAME} duckdb_rsj_whole_plan_wo_stats_breakdown_${LOG_NAME}
###### without updating statistics


####### join order opt + split
echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_MEASURE_EXE_TIME=1" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make >> compile.log 2>&1 && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} duckdb_js_breakdown_${LOG_NAME}

###### reorder table + split + join order opt
#echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_MEASURE_EXE_TIME=1" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make >> compile.log 2>&1 && cd measure/
#for sql in "${dir}"/*; do
#  echo "execute ${sql}" >> ${LOG_NAME};
#  for i in $(eval echo {1.."${iteration}"}); do
#    echo -ne ".read ${sql}" | duckdb ./imdb.db;
#  done
#done
#mv ${LOG_NAME} duckdb_rsj_breakdown_${LOG_NAME}


####### join order opt + split + merge back to the whole plan
echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_MERGE_BACK_PLAN=1 ENABLE_CROSS_PRODUCT_REWRITE=0" 2>&1|tee -a compile.log
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_MERGE_BACK_PLAN=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make >> compile.log 2>&1 && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} duckdb_js_whole_plan_breakdown_${LOG_NAME}

###### reorder table + split + join order opt + merge back to the whole plan
#echo "compile with ENABLE_QUERY_SPLIT=1 ENABLE_MERGE_BACK_PLAN=1 ENABLE_CROSS_PRODUCT_REWRITE=1" 2>&1|tee -a compile.log
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_MERGE_BACK_PLAN=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make >> compile.log 2>&1 && cd measure/
#for sql in "${dir}"/*; do
#  echo "execute ${sql}" >> ${LOG_NAME};
#  for i in $(eval echo {1.."${iteration}"}); do
#    echo -ne ".read ${sql}" | duckdb ./imdb.db;
#  done
#done
#mv ${LOG_NAME} duckdb_rsj_whole_plan_breakdown_${LOG_NAME}


mv *${LOG_NAME} job_result/.
