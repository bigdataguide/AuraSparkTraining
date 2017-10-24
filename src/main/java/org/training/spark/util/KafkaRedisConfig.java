package org.training.spark.util;

/**
 * Created by xicheng.dong on 10/23/17.
 */
public class KafkaRedisConfig {
    public static String REDIS_SERVER = "localhost";
    public static int REDIS_PORT = 6379;

    public static String KAFKA_SERVER = "localhost";
    public static String KAFKA_ADDR = KAFKA_SERVER + ":9092";
    public static String KAFKA_USER_TOPIC = "user_events";

    public static String ZOOKEEPER_SERVER = "localhost:2181";
    public static String ZOOKEEPER_PATH = "/offsets";

}
