package org.training.spark.streaming.stores;

import kafka.common.TopicAndPartition;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

/**
 * Created by xicheng.dong on 10/24/17.
 */
public interface OffsetsStore {
    public Map<TopicAndPartition, Long> readOffsets(String topic);

    public void saveOffsets(String topic, JavaRDD rdd);
}
