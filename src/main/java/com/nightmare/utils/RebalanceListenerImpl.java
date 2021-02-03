package com.nightmare.utils;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public class RebalanceListenerImpl implements ConsumerRebalanceListener {
    private KafkaConsumer consumer;
    private String name;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

    public RebalanceListenerImpl(KafkaConsumer con, String name) {
        this.consumer = con;
        this.name = name;
    }

    public void addOffset(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Assigned to consumer [" + name + "]");
        for (TopicPartition partition : partitions)
            System.out.println(partition.partition() + ",");
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Revoked from consumer [" + name + "]");
        for (TopicPartition partition : partitions)
            System.out.println(partition.partition() + ",");


        System.out.println("Following Partitions committed by consumer [" + name + "]");
        for (TopicPartition tp : currentOffsets.keySet())
            System.out.println(tp.partition());

        consumer.commitSync(currentOffsets);
        currentOffsets.clear();
    }
}
