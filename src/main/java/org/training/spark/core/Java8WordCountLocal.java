package org.training.spark.core;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Java8WordCountLocal {
  public static void main(String[] args) throws Exception {
    String masterUrl = "local[1]";
    String inputFile = "data/textfile";
    String outputFile = "output";

    // Create a Java Spark Context.
    SparkConf conf = new SparkConf().setAppName("wordCount");
    conf.setMaster(masterUrl);

    JavaSparkContext sc = new JavaSparkContext(conf);
    // Load our input data.
    JavaRDD<String> input = sc.textFile(inputFile);

    // Split up into words.
    JavaRDD<String> words = input.flatMap( x ->
        Arrays.asList(x.split(" ")).iterator()
    ).filter( s -> s.length() > 1);

    // Transform into word and count.
    JavaPairRDD<String, Integer> counts = words.mapToPair( x ->
        new Tuple2<String, Integer>(x, 1)
    ).reduceByKey((x, y) -> x + y);

    // Save the word count back out to a text file, causing evaluation.
    Path outputPath = new Path(outputFile);
    FileSystem fs = outputPath.getFileSystem(new HdfsConfiguration());
    if(fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    counts.saveAsTextFile(outputFile);

    // Just for debugging, NOT FOR PRODUCTION
    counts.foreach( pair ->
        System.out.println(String.format("%s - %d", pair._1(), pair._2()))
    );
  }
}