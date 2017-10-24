package org.training.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * 看过“Lord of the Rings, The (1978)”用户和年龄性别分布
 */
public class SparkMovieUserAnalyzer {

    public static void main(String[] args) {
        String dataPath = "data/ml-1m";

        SparkConf conf = new SparkConf().setAppName("PopularMovieAnalyzer");
        if(args.length > 0) {
            dataPath = args[0];
        } else {
            conf.setMaster("local[1]");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        /**
         * Step 1: Create RDDs
         */
        String DATA_PATH = dataPath;
        String MOVIE_TITLE = "Lord of the Rings, The (1978)";
        final String MOVIE_ID = "2116";

        JavaRDD<String> usersRdd = sc.textFile(DATA_PATH + "/users.dat");
        JavaRDD<String> ratingsRdd = sc.textFile(DATA_PATH + "/ratings.dat");

        /**
         * Step 2: Extract columns from RDDs
         */
        //users: RDD[(userID, (gender, age))]
        JavaPairRDD<String, String> users = usersRdd.mapToPair(
                new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] line = s.split("::");
                return new Tuple2<>(line[0], line[1] + ":" + line[2]);
                }
            });

        //usermovie: RDD[(userID, movieID)]
        JavaPairRDD<String, String> usermovie = ratingsRdd.mapToPair(
                new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) {
                String[] line = s.split("::");
                return new Tuple2<>(line[0], line[1]);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            public Boolean call(Tuple2<String, String> t) {
                return t._2().equals(MOVIE_ID);
            }
        });

        /**
         * Step 3: join RDDs
         */
        //useRating: RDD[(userID, (movieID, (gender, age))]
        JavaPairRDD<String, Tuple2<String, String>> userRating = usermovie.join(users);

        //movieuser: RDD[(movieID, (movieTile, (gender, age))]
        JavaPairRDD<String, Integer> userDistribution = userRating.mapToPair(
                new PairFunction<Tuple2<String,Tuple2<String,String>>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<String, Tuple2<String, String>> t) {
                        return new Tuple2<>(t._2()._2(), 1);
                    }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer n1, Integer n2) throws Exception {
                return n1 + n2;
            }
        });

        for (Tuple2<String, Integer> t : userDistribution.collect()){
         System.out.println("gender & age:" + t._1() + ", count:" + t._2());
        }

        sc.stop();
    }
}
