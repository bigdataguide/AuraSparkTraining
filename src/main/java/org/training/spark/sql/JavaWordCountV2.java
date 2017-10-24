package org.training.spark.sql;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by xicheng.dong on 10/23/17.
 */
public class JavaWordCountV2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().getOrCreate();

        Dataset<String> ds = spark.read().textFile("data/textfile/Hamlet.txt");
        Dataset<String> words = ds.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }, Encoders.STRING());

        Dataset<Row> result = words
                .toDF("word")
                .groupBy("word")
                .count();

        result.show();

        words.createOrReplaceTempView("words");
        spark.sql("select value, count(*) from words group by value").show();


    }
}
