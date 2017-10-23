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
      .groupBy("value")
      .count()

    result.collect().foreach(println)

    result.toDF("w", "c").createOrReplaceTempView("words")

    // print top 10 not null words
    spark.sql("select w from words where length(w) > 0 order by c desc limit 10")
      .collect().foreach(println)
  }
}
