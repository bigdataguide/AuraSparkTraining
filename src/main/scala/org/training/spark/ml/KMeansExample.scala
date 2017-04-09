package org.training.spark.ml

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession

object KMeansExample {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[1]"
    var dataPath = "data/mllib/sample_kmeans_data.txt"
    if (args.length > 0) {
      masterUrl = args(0)
    } else if(args.length > 1) {
      dataPath = args(1)
    }

    val spark = SparkSession
      .builder
      .master(masterUrl)
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm").load(dataPath)

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$

    spark.stop()
  }
}
