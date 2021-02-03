package com.nightmare.producers;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/*
 * A very dirty code to demonstrate the rebalance example properly so that records are evenly distributed
 * */
public class YetAnotherCustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (key == null) {
            return 0;
        }

        // Integer keyValue = deserialize((byte[]) key);

        if (String.valueOf(key).endsWith("1") || String.valueOf(key).endsWith("2"))
            return 0;
        else if (String.valueOf(key).endsWith("3") || String.valueOf(key).endsWith("4"))
            return 1;
        else if (String.valueOf(key).endsWith("5") || String.valueOf(key).endsWith("6"))
            return 2;
        else if (String.valueOf(key).endsWith("7") || String.valueOf(key).endsWith("8"))
            return 3;
        else
            return 4;

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
    }

    /*private static <V> V deserialize(final byte[] objectData) {
        return (V) org.apache.commons.lang3.SerializationUtils.deserialize(objectData);
    }*/
}
