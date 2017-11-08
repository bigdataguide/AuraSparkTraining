package org.training.spark.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.training.spark.streaming.stores.OffsetsStore;
import org.training.spark.streaming.stores.ZooKeeperOffsetsStore;
import org.training.spark.util.KafkaRedisConfig;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by xicheng.dong on 10/24/17.
 */
public class RecorvableWordCount {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("UserClickCountAnalytics");
        if (args.length == 0) {
            conf.setMaster("local[1]");
        }
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Kafka configurations
        String[] topics = KafkaRedisConfig.KAFKA_USER_TOPIC.split("\\,");
        System.out.println("Topics: " + Arrays.toString(topics));

        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        OffsetsStore offsetsStore = new ZooKeeperOffsetsStore(
                KafkaRedisConfig.ZOOKEEPER_SERVER, KafkaRedisConfig.ZOOKEEPER_PATH);
        Map<TopicAndPartition, Long> offsets = offsetsStore.readOffsets(topics[0]);
        JavaDStream<String> kafkaStream = null;
        if(offsets == null) {
            // Create a direct stream
            kafkaStream = KafkaUtils.createDirectStream(ssc,
                    String.class, String.class,
                    StringDecoder.class, StringDecoder.class,
                    kafkaParams,
                    new HashSet<String>(Arrays.asList(topics)))
                    .map(new Function<Tuple2<String, String>, String>() {
                @Override
                public String call(Tuple2<String, String> line) throws Exception {
                    return line._2();
                }
            });
        } else {
            Function<MessageAndMetadata<String, String>, String> func =
                    new Function<MessageAndMetadata<String, String>, String>() {
                @Override
                public String call(MessageAndMetadata<String, String> msg) throws Exception {
                    return msg.message();
                }
            };
            kafkaStream = KafkaUtils.createDirectStream(ssc,
                    String.class, String.class,
                    StringDecoder.class, StringDecoder.class,
                    String.class,
                    kafkaParams,
                    offsets,
                    func);
        }

        kafkaStream.map(new Function<String, JSONObject>() {
            public JSONObject call(String s) {
                return JSON.parseObject(s);
            }
        }).print(100);

        ssc.start();
        ssc.awaitTermination();
    }
}
