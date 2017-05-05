package org.training.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.training.spark.util.MLExampleUtils

import scala.language.reflectiveCalls

object LinearRegressionExample {

  def main(args: Array[String]) {
    var dataPath = "data/mllib/sample_linear_regression_data.txt"
    val conf = new SparkConf()
    if(args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("LinearRegressionExample")
      .getOrCreate()

    val input: String = dataPath
    val testInput: String = ""
    val dataFormat: String = "libsvm"
    val regParam: Double = 0.0
    val elasticNetParam: Double = 0.0
    val maxIter: Int = 100
    val tol: Double = 1E-6
    val fracTest: Double = 0.2

    // Load training and test data and cache it.
    val (training: DataFrame, test: DataFrame) = MLExampleUtils.loadDatasets(input,
      dataFormat, testInput, "regression", fracTest)

    val lir = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setMaxIter(maxIter)
      .setTol(tol)

    // Train the model
    val startTime = System.nanoTime()
    val lirModel = lir.fit(training)
    val elapsedTime = (System.nanoTime() - startTime) / 1e9
    println(s"Training time: $elapsedTime seconds")

    // Print the weights and intercept for linear regression.
    println(s"Weights: ${lirModel.coefficients} Intercept: ${lirModel.intercept}")

    println("Training data results:")
    MLExampleUtils.evaluateRegressionModel(lirModel, training, "label")
    println("Test data results:")
    MLExampleUtils.evaluateRegressionModel(lirModel, test, "label")

    spark.stop()
  }
}