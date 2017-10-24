package org.training.spark.streaming.stores;

import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.apache.zookeeper.data.Stat;
import scala.Option;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xicheng.dong on 10/24/17.
 */
public class ZooKeeperOffsetsStore implements OffsetsStore {
    private String zkServers;
    private String zkPath;
    private ZkClient zkClient;

    public ZooKeeperOffsetsStore(String zkServers, String zkPath) {
        this.zkServers = zkServers;
        this.zkPath = zkPath;
        this.zkClient = new ZkClient(zkServers, 10000, 10000);
    }

    @Override
    public Map<TopicAndPartition, Long> readOffsets(String topic) {
        Map<TopicAndPartition, Long> result = new HashMap<>();

        Tuple2<Option<String>, Stat> tuple = ZkUtils.readDataMaybeNull(zkClient, zkPath);

        String offsetStr = tuple._1().get();
        if (offsetStr != null) {
            String[] offsets = offsetStr.split(",");
            for (String partition : offsets) {
                String[] par = partition.split(":");
                result.put(new TopicAndPartition(topic, Integer.parseInt(par[0])),
                        Long.parseLong(par[1]));
            }
        }
        return result;
    }

    @Override
    public void saveOffsets(String topic, JavaRDD rdd) {
        OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd).offsetRanges();
        StringBuilder result = new StringBuilder();
        for (OffsetRange offsetRange : offsetRanges) {
            result.append(offsetRange.partition() + ":" + offsetRange.fromOffset());
            result.append(",");
        }

        String data = result.substring(0, result.length() - 1).toString();
        ZkUtils.updatePersistentPath(zkClient, zkPath, data);
    }
}