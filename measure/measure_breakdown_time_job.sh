#!/bin/bash

# execute queries
dir="/home/pei/Project/benchmarks/imdb_job-postgres/queries"
iteration=15 # 5 warm up, 10 runs

LOG_NAME=time_log.csv

rm -rf *${LOG_NAME}
rm -rf job_result/*${LOG_NAME}


###### official duckdb
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make && cd measure/
echo "PreOptimize, final-PostOptimize, final-CreatePlan, Execute"  >> ${LOG_NAME};
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} official_breakdown_${LOG_NAME}


###### without updating statistics
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MEASURE_EXE_TIME=1 VERBOSE=1 make && cd measure/
#for sql in "${dir}"/*; do
#  echo "execute ${sql}" >> ${LOG_NAME};
#  for i in $(eval echo {1.."${iteration}"}); do
#    echo -ne ".read ${sql}" | duckdb ./imdb.db;
#  done
#done
#mv ${LOG_NAME} js_wo_stats_breakdown_${LOG_NAME}

cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MEASURE_EXE_TIME=1 VERBOSE=1 make && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} rsj_wo_stats_breakdown_${LOG_NAME}

####### join order opt + split + merge back to the whole plan
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make && cd measure/
#for sql in "${dir}"/*; do
#  echo "execute ${sql}" >> ${LOG_NAME};
#  for i in $(eval echo {1.."${iteration}"}); do
#    echo -ne ".read ${sql}" | duckdb ./imdb.db;
#  done
#done
#mv ${LOG_NAME} js_whole_plan_breakdown_${LOG_NAME}

###### reorder table + split + join order opt + merge back to the whole plan
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} rsj_whole_plan_wo_stats_breakdown_${LOG_NAME}
###### without updating statistics


####### join order opt + split
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make && cd measure/
#for sql in "${dir}"/*; do
#  echo "execute ${sql}" >> ${LOG_NAME};
#  for i in $(eval echo {1.."${iteration}"}); do
#    echo -ne ".read ${sql}" | duckdb ./imdb.db;
#  done
#done
#mv ${LOG_NAME} js_breakdown_${LOG_NAME}

###### reorder table + split + join order opt
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} rsj_breakdown_${LOG_NAME}


####### join order opt + split + merge back to the whole plan
#cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_MERGE_BACK_PLAN=1 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 make && cd measure/
#for sql in "${dir}"/*; do
#  echo "execute ${sql}" >> ${LOG_NAME};
#  for i in $(eval echo {1.."${iteration}"}); do
#    echo -ne ".read ${sql}" | duckdb ./imdb.db;
#  done
#done
#mv ${LOG_NAME} js_whole_plan_breakdown_${LOG_NAME}

###### reorder table + split + join order opt + merge back to the whole plan
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_MERGE_BACK_PLAN=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make && cd measure/
for sql in "${dir}"/*; do
  echo "execute ${sql}" >> ${LOG_NAME};
  for i in $(eval echo {1.."${iteration}"}); do
    echo -ne ".read ${sql}" | duckdb ./imdb.db;
  done
done
mv ${LOG_NAME} rsj_whole_plan_breakdown_${LOG_NAME}


mv *${LOG_NAME} job_result/.
