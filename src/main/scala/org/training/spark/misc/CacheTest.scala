package org.training.spark.misc

import org.apache.spark.{SparkConf, SparkContext}

object CacheTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Cache Test")
    val sc = new SparkContext(conf)

    val input = if (args.length > 0) args(0) else "data/access.log"

    val lines = sc.textFile(input)

    val t1 = time {
      lines.filter(_.startsWith("200")).count()
    }
    val t2 = time {
      lines.filter(_.startsWith("404")).count()
    }
    val t3 = time {
      lines.filter(_.startsWith("503")).count()
    }

    println(s"without cache:\n t1: $t1 ms, t2: $t2 ms, t3: $t3 ms")

    lines.cache()
    val t11 = time {
      lines.filter(_.startsWith("200")).count()
    }
    val t12 = time {
      lines.filter(_.startsWith("404")).count()
    }
    val t13 = time {
      lines.filter(_.startsWith("503")).count()
    }

    println(s"with cache:\n t11: $t11 ms, t12: $t12 ms, t3: $t13 ms")

  }

  def time(body: => Unit): Double = {
    val start = System.nanoTime()
    body
    val end = System.nanoTime()
    (end - start) / 1E6
  }

}
