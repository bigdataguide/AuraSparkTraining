package org.training.spark.streaming;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.training.spark.util.KafkaRedisConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class JavaKafkaEventProducer {
    private static String[] users = {
            "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
            "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
            "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
            "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
            "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d"};

    private static Random random = new Random();

    private static int pointer = -1;

    private static String getUserID() {
        pointer = pointer + 1;
        if(pointer >= users.length) {
            pointer = 0;
            return users[pointer];
        } else {
            return users[pointer];
        }
    }

    private static double click() {
        return random.nextInt(10);
    }

    // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --create --topic user_events --replication-factor 2 --partitions 2
    // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --list
    // bin/kafka-topics.sh --zookeeper zk1:2181,zk2:2181,zk3:2181/kafka --describe user_events
    // bin/kafka-console-consumer.sh --zookeeper zk1:2181,zk2:2181,zk3:22181/kafka --topic test_json_basis_event --from-beginning
    public static void main(String[] args) throws Exception {
        String topic = KafkaRedisConfig.KAFKA_USER_TOPIC;
        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> props = new HashMap<>();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(props);

        while(true) {
            // prepare event data
            JSONObject event = new JSONObject();
            event.put("uid", getUserID());
            event.put("event_time", System.currentTimeMillis());
            event.put("os_type", "Android");
            event.put("click_count", click());

            // produce event message
            producer.send(new ProducerRecord(topic, event.toString()));
            System.out.println("Message sent: " + event);

            Thread.sleep(1000);
        }
    }
}