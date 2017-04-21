cp target/Spark-Perf-1.0-SNAPSHOT.jar /proj/spark-heron-PG0/rohit/temp/Spark-Perf-1.0-SNAPSHOT.jar
~/spark-2.0.1-bin-hadoop2.7/bin/spark-submit \
    --class  edu.wisc.streaming.RandomWordCount \
    --master spark://ms1127.utah.cloudlab.us:7077 \
    --deploy-mode cluster \
    --conf="spark.driver.cores=2" \
    --conf="spark.streaming.backpressure.enabled=true" \
    --conf="spark.streaming.receiver.maxRate=2000000" \
/proj/spark-heron-PG0/rohit/temp/Spark-Perf-1.0-SNAPSHOT.jar \
   200 5
