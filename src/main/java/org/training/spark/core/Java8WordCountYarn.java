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

/**
 *  How to submit to Yarn.
 *
 spark-submit \
 --master yarn \
 --deploy-mode client \
 --class org.training.spark.core.Java8WordCountYarn \
 --name wordcount \
 --driver-memory 1g \
 --executor-memory 1g \
 --executor-cores 1 \
 --num-executors 2 \
 AuraSparkTraining-1.0-SNAPSHOT.jar [input] [output]
 */
public class Java8WordCountYarn {
  public static void main(String[] args) throws Exception {

    String inputFile = "hdfs:///tmp/input";
    String outputFile = "hdfs:///tmp/output";


    if (args.length > 1) {
      inputFile = args[0];
      outputFile = args[1];
    }
    // Create a Java Spark Context.
    SparkConf conf = new SparkConf().setAppName("wordCount");

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
    counts.cache();
    counts.saveAsTextFile(outputFile);

    // Just for debugging, NOT FOR PRODUCTION
    counts.foreach( pair ->
        System.out.println(String.format("%s - %d", pair._1(), pair._2()))
    );
  }
}