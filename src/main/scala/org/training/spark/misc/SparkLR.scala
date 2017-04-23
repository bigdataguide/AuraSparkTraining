package org.training.spark.misc

import java.util.Random

import breeze.linalg.{DenseVector, Vector}
import org.apache.spark._

import scala.math.exp

/**
  * Logistic regression based classification.
  * Usage: SparkLR [slices]
  *
  * This is an example implementation for learning how to use Spark. For more conventional use,
  * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
  * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
  */
object SparkLR {
  val N = 10000
  // Number of data points
  val D = 10
  // Numer of dimensions
  val R = 0.7
  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def generateData: Array[DataPoint] = {
    def generatePoint(i: Int): DataPoint = {
      val y = if (i % 2 == 0) -1 else 1
      val x = DenseVector.fill(D) {
        rand.nextGaussian + y * R
      }
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of Logistic Regression and is given as an example!
        |Please use either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
        |org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]) {

    showWarning()

    val sparkConf = new SparkConf().setAppName("SparkLR")
    val sc = new SparkContext(sparkConf)
    val numSlices = if (args.length > 0) args(0).toInt else 2
    val points = sc.parallelize(generateData, numSlices).cache()

    // Initialize w to a random value
    var w = DenseVector.fill(D) {
      2 * rand.nextDouble - 1
    }
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i + ",w:" + w)
      val gradient = points.map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)

    sc.stop()
  }
}