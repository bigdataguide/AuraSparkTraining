package org.training.spark.ml;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * An example demonstrating k-means clustering.
 * Run with
 * <pre>
 * bin/run-example ml.JavaKMeansExample
 * </pre>
 */
public class JavaKMeansExample {

  public static void main(String[] args) {
    // Create a SparkSession.
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaKMeansExample")
      .getOrCreate();

    // Loads data.
    Dataset<Row> dataset = spark.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");

    // Trains a k-means model.
    KMeans kmeans = new KMeans().setK(2).setSeed(1L);
    KMeansModel model = kmeans.fit(dataset);

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    double WSSSE = model.computeCost(dataset);
    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

    // Shows the result.
    Vector[] centers = model.clusterCenters();
    System.out.println("Cluster Centers: ");
    for (Vector center: centers) {
      System.out.println(center);
    }

    spark.stop();
  }
}
