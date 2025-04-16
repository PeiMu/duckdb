## Compilation
```bash
# official
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=0 VERBOSE=1 make

# AQP
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 VERBOSE=1 make

# AQP w/o updating statistics
cd ../ && make clean && GEN=ninja ENABLE_QUERY_SPLIT=1 ENABLE_CROSS_PRODUCT_REWRITE=1 ENABLE_SPECIFY_EST_STAT=1 VERBOSE=1 make
```

### Compilation with Execution Breakdown
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
bash ./measure_job.sh && bash ./measure_breakdown_time_job.sh

# measure DSB
bash ./measure_dsb.sh && bash ./measure_breakdown_time_dsb.sh
```

## Test
```bash
# check JOB
bash ./check_result_job.sh
diff job_result/duckdb_job_result_official.txt job_result/duckdb_job_result_rsj.txt

# check DSB
bash ./check_result_dsb.sh
diff dsb_result/duckdb_result_dsb_official.txt dsb_result/duckdb_result_dsb_rsj.txt
```
