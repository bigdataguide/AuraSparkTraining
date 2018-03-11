package org.training.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xicheng.dong on 10/24/17.
 */
public class SparkSQLMovieUserAnalyzer {
    public static void main(String[] args) {
        String dataPath = "data/ml-1m";

        SparkConf conf = new SparkConf().setAppName("PopularMovieAnalyzer");
        if(args.length > 0) {
            dataPath = args[0];
        } else {
            conf.setMaster("local[1]");
        }
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> userDS = spark.read().json(dataPath + "/users.json");
        Dataset<Row> ratingDS = spark.read().parquet(dataPath + "/ratings.parquet");
        ratingDS.filter("movieID = 2116")
                .join(userDS,"userID" )
                .select("gender", "age")
                .groupBy("gender", "age")
                .count()
                .show();
        userDS.createOrReplaceTempView("user");
        ratingDS.createOrReplaceTempView("rating");
        spark.sql("SELECT gender, age, count(*) from user as u join rating  as r " +
                "on u.userid = r.userid where movieid = 2116 group by gender, age ").show();

        // Save to different format and compare their size
        ratingDS.write().mode("overwrite").csv("/tmp/csv");
        ratingDS.write().mode("overwrite").json("/tmp/json");
        ratingDS.write().mode("overwrite").parquet("/tmp/parquet");
        ratingDS.write().mode("overwrite").orc("/tmp/orc");

        spark.stop();
    }
}
