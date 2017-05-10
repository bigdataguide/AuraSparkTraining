package org.training.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCountV2 {
  def main(args: Array[String]) {
    var dataPath = "data/textfile"
    val conf = new SparkConf()
    if (args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val spark = SparkSession
      .builder()
      .appName("WordCountV2")
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val result = spark.read.textFile(dataPath)
      .flatMap(_.split("\\s+"))
      .groupByKey(x => x)
      .count()

    result.collect().foreach(println)
  }
}
