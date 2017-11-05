package org.training.spark.streaming

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {
     def main(args: Array[String]) {
      if (args.length < 1) {
        System.err.println("Usage: HdfsWordCount <directory>")
        System.exit(1)
      }

    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    println("input:" + args(0))
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    wordCounts.print(10)
    val wordCounts = lines.transform(rdd => {
      val result = rdd
        .flatMap(_.split(" "))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
      result
    })
    wordCounts.saveAsTextFiles("/tmp/output/result","txt")
    lines.foreachRDD(rdd => {
      val result = rdd
        .flatMap(_.split(" "))
        .map(x => (x, 1))
        .reduceByKey(_ + _)
      result.take(10).foreach(println)
    })
    wordCounts.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val tableName = "hamlet"
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "bigdata:2181")
        val htable = new HTable(hbaseConf, TableName.valueOf(tableName))
        partitionOfRecords.foreach(pair => {
          val put = new Put(Bytes.toBytes(rdd.id))
          put.add("words".getBytes, Bytes.toBytes(pair._1), Bytes.toBytes(pair._2))
          htable.put(put)
        })
        htable.close
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}


