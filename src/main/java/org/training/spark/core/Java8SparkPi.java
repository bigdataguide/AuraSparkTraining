package org.training.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

public class Java8SparkPi {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Java Spark Pi");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        int slices = 2;
        if (args.length > 0) {
            slices = Integer.parseInt(args[0]);
        }
        int n = (int)Math.min(100000L * slices, Integer.MAX_VALUE);

        List<Integer> list = new ArrayList<Integer>(n);
        for (int i = 1;i < n;i++) {
            list.add(i);
        }

        JavaRDD<Integer> rdd = jsc.parallelize(list, slices);

        JavaRDD<Integer> mapRdd = rdd.map(v1 -> {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y <= 1) ? 1 : 0;
        });

        int count = mapRdd.reduce((v1, v2) -> v1 + v2);

        System.out.println("Pi is roughly " + 4.0 * count/(n - 1));
        jsc.stop();
    }

}
