package org.training.spark.misc

import org.apache.spark.sql.SparkSession

object BroadcastTest {
  def main(args: Array[String]) {

    val blockSize = if (args.length > 2) args(2) else "4096"

    val spark = SparkSession
      .builder()
      .appName("Broadcast Test")
      .config("spark.broadcast.blockSize", blockSize)
      .getOrCreate()

    val sc = spark.sparkContext

    val slices = if (args.length > 0) args(0).toInt else 2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = (0 until num).toArray
    val barr1 = sc.broadcast(arr1)

    for (i <- 0 until 3) {
      println("Iteration " + i)
      println("===========")
      val startTime = System.nanoTime
      val observedSizes =
        sc.parallelize(1 to 10, slices).map(_ => barr1.value.length)
      // Collect the small RDD so we can print the observed sizes locally.
      observedSizes.collect().foreach(i => println(i))
      println(
        "Iteration %d took %.0f milliseconds"
          .format(i, (System.nanoTime - startTime) / 1E6))
    }

    spark.stop()
  }
}
