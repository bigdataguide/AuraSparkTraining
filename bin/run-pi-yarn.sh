#!/usr/bin/env bash

FWD="$(cd "`dirname "$0"`";cd ..;pwd)"

if [ -z "$SPARK_HOME" ]; then
     echo "SPARK_HOME is not set, Please set SPARK_HOME first" 1>&2
     exit -1
fi

"$SPARK_HOME"/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class org.training.spark.misc.SparkPi \
    --name sparkpi \
    --driver-memory 1g \
    --driver-cores 1 \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 1 \
    --conf spark.pi.iterators=50000 \
    --conf spark.pi.slice=10 \
    --queue default \
    "$FWD"/target/AuraSparkTraining-1.0-SNAPSHOT-jar-with-dependencies.jar