export RUN_LOG_TIMINGS=1
export SCALE_FACTOR=1.0
export PROJECT_ID=velascoluis-dev-sandbox
export DATASET_ID=bigframes_tpch_$(printf "%.0f" ${SCALE_FACTOR})

echo ${SCALE_FACTOR}
echo ${PROJECT_ID}
echo ${DATASET_ID}

echo run with cached IO
make tables
make load-tables-bq
make run-bigframes