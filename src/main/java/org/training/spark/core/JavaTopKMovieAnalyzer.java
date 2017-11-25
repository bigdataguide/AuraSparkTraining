package org.training.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

/**
 * 得分最高的10部电影；看过电影最多的前10个人；女性看多最多的10部电影；男性看过最多的10部电影
 */
public class JavaTopKMovieAnalyzer {

  public static void main(String[] args) {
    String dataPath = "data/ml-1m";
    SparkConf conf = new SparkConf().setAppName("TopKMovieAnalyzer");
    if (args.length > 0) {
      dataPath = args[0];
    } else {
      conf.setMaster("local[1]");
    }

    JavaSparkContext sc = new JavaSparkContext(conf);

    /**
     * Step 1: Create RDDs
     */
    String DATA_PATH = dataPath;

    JavaRDD<String> ratingsRdd = sc.textFile(DATA_PATH + "/ratings.dat");

    /**
     * Step 2: Extract columns from RDDs
     */
    //users: RDD[(userID, movieID, score)]
    JavaRDD<Tuple3<String, String, String>> ratings = ratingsRdd
        .map(x -> x.split("::"))
        .map(x -> new Tuple3<>(x[0], x[1], x[2]))
        .cache();

    /**
     * Step 3: analyze result
     */
    ratings
        .mapToPair(x ->
            new Tuple2<String, Tuple2<Integer, Integer>>(x._1(),
                new Tuple2<>(Integer.parseInt(x._3()), 1)))
        .reduceByKey((v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2))
        .mapToPair(x -> new Tuple2<>(x._2._1 / x._2._2 + 0.0f, x._1))
        .sortByKey(false)
        .take(10)
        .forEach(x -> System.out.println(x));

    ratings
        .mapToPair(x -> new Tuple2<String, Integer>(x._1(), 1))
        .reduceByKey((x, y) -> x + y)
        .mapToPair(x -> new Tuple2<>(x._2, x._1))
        .sortByKey(false)
        .take(10)
        .stream()
        .forEach(x -> System.out.println(x));

    sc.stop();
  }
}
