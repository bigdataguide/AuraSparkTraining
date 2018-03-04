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
        SparkSession spark = SparkSession.builder()
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.sql.autoBroadcastJoinThreshold", 20485760)
            .getOrCreate();

        String inputPath = "data/textfile/Hamlet.txt";
        if(args.length > 0) {
            inputPath = args[0];
        }
        Dataset<String> ds = spark.read().textFile(inputPath);

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
