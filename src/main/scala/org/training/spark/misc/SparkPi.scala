package org.training.spark.misc

import org.apache.spark.{SparkContext, SparkConf}

import scala.math.random

object SparkPi {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Spark Pi")
    val sc = new SparkContext(conf)

    val iterNum = conf.getLong("spark.pi.iterators", 100000L)
    val slices = conf.getInt("spark.pi.slices", 2)
    val n = math.min(iterNum * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc
      .parallelize(1 until n, slices)
      .map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }
      .reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / (n - 1))

    sc.stop()
  }
}
