#!/usr/bin/env bash

FWD="$(cd "`dirname "$0"`";cd ..;pwd)"

if [ -z "$SPARK_HOME" ]; then
     echo "SPARK_HOME is not set, Please set SPARK_HOME first" 1>&2
     exit -1
fi

"$SPARK_HOME"/bin/spark-submit \
    --master local \
    --class org.training.spark.misc.SparkPi \
    --name sparkpi \
    "$FWD"/target/AuraSparkTraining-1.0-SNAPSHOT-jar-with-dependencies.jar
