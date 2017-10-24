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
    public static Dataset<Row> getUserDS(SparkSession spark, String dataPath) {
        JavaRDD<String> userRDD = spark.sparkContext()
                .textFile(dataPath + "/users.dat", 1)
                .toJavaRDD();

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : "userID gender age".split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> userRowRDD = userRDD.map(new Function<String, Row>() {
            public Row call(String s) {
                String[] attr = s.split("::");
                return RowFactory.create(attr[0], attr[1], attr[2]);
            }
        });
        return spark.createDataFrame(userRowRDD, schema);
    }

    public static Dataset<Row> getRatingDS(SparkSession spark, String dataPath) {
        JavaRDD<String> ratingRDD = spark.sparkContext()
                .textFile(dataPath + "/ratings.dat", 1)
                .toJavaRDD();

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : "userID movieID".split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> ratingRowRDD = ratingRDD.map(new Function<String, Row>() {
            public Row call(String s) {
                String[] attr = s.split("::");
                return RowFactory.create(attr[0], attr[1]);
            }
        });
        return spark.createDataFrame(ratingRowRDD, schema);
    }

    public static void main(String[] args) {
        String dataPath = "data/ml-1m";

        SparkConf conf = new SparkConf().setAppName("PopularMovieAnalyzer");
        if(args.length > 0) {
            dataPath = args[0];
        } else {
            conf.setMaster("local[1]");
        }
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> userDS = getUserDS(spark, dataPath);
        Dataset<Row> ratingDS = getRatingDS(spark, dataPath);

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

        spark.stop();
    }
}
