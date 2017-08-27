package org.training.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xicheng.dong on 7/1/17.
  */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Wordcount")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("data/textfile")

    val rdd2 = rdd.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_+_)
    rdd2
      .filter(_._1.length > 0)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .take(50)
      .foreach(println)
    //rdd2.take(10).foreach(x=> println(x))

    val input = sc.parallelize(
      List(
      ("coffee", 1) ,
      ("coffee", 3) ,
      ("panda",4),
      ("coffee", 5),
      ("street", 2),
      ("panda", 5)))

    input.groupByKey().map(x => (x._1, x._2.sum.toDouble/x._2.size)).foreach(println)
  }
}
