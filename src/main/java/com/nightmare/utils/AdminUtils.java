package com.nightmare.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class AdminUtils {

    public static void main(String[] args) {
        try {
            String topicName="topic-created-from-code-3";
            Properties props=new Properties();
            props.put("bootstrap.servers", "192.168.118.115:9092");
            final AdminClient client = AdminClient.create(props);
            final NewTopic topic = new NewTopic(topicName, 1, (short) 1);
            final Set setofTopics = Collections.singleton(topic);
            final CreateTopicsResult result = client.createTopics(setofTopics);
            result.values().get(topicName).get();
            System.out.println("Topic Created");
        } catch (InterruptedException | ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e.getMessage(), e);
            } else {
                System.out.println("Topic created");
            }

        }
    }
}
