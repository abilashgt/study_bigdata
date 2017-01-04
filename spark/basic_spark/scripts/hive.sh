spark-submit --class examples.HiveExample \
--num-executors 80 --executor-cores 4 --executor-memory 16g --driver-memory 4g --master yarn-client --conf spark.yarn.executor.memoryOverhead=4000 \
../target/basic-spark.jar
