## Compilation
```bash
# official
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make

# AQP
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make

# AQP w/o updating statistics
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make
```

### Compilation with Performance Breakdown
```bash
# official
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 ENABLE_CROSS_PRODUCT_REWRITE=0 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make

# AQP
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 ENABLE_MEASURE_EXE_TIME=1 make

# AQP w/o updating statistics
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MEASURE_EXE_TIME=1 VERBOSE=1 make
```

### Compilation with Merging Back
```bash
# AQP
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make

# AQP w/o updating statistics
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 ENABLE_MERGE_BACK_PLAN=1 VERBOSE=1 make
```

## Measure Performance
```bash
# measure JOB
sudo rm -rf job_result/
bash ./measure_job.sh && bash ./measure_breakdown_time_job.sh

# measure DSB
sudo rm -rf dsb_result_10/
bash ./measure_dsb.sh 10 && bash ./measure_breakdown_time_dsb.sh 10

sudo rm -rf dsb_result_100/
bash ./measure_dsb.sh 100 && bash ./measure_breakdown_time_dsb.sh 100
```

## Test
```bash
# check JOB
bash ./check_result_job.sh
diff job_result/duckdb_job_result_official.txt job_result/duckdb_job_result_rsj.txt

# check DSB
bash ./check_result_dsb.sh 10
diff dsb_10_result/duckdb_result_dsb_10_official.txt dsb_10_result/duckdb_result_dsb_10_rsj.txt

bash ./check_result_dsb.sh 100
diff dsb_100_result/duckdb_result_dsb_100_official.txt dsb_100_result/duckdb_result_dsb_100_rsj.txt
```
