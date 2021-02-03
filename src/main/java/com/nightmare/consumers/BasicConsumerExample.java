/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nightmare.consumers;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class BasicConsumerExample {

    public static void main(String[] args) {


        try {
            boolean syncCommit = true;
            String topic = "streams-wordcount-output";
            Properties consumerConfig = new Properties();
            consumerConfig.put("group.id", "stream-group");
            consumerConfig.put("bootstrap.servers", "192.168.118.125:9092");
            consumerConfig.put("auto.offset.reset", "earliest");
            //consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (records == null || records.isEmpty()) {
                   // System.out.println("No new messages to read from. Exiting");
                    continue;
                }
                for (ConsumerRecord<String, String> record : records) {
                    //System.out.printf("Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    System.out.printf("key = %s, value = %s\n", record.key(), record.value());
                }
                if (syncCommit) {
                    consumer.commitSync();
                } else {
                    //consumer.commitAsync();
                    consumer.commitAsync(new DummyConsumerCallback());
                }
                // System.out.println("Committed");
               // Thread.sleep(2000);
            }

        } catch (Exception e) {

        }

    }

}

class DummyConsumerCallback implements OffsetCommitCallback {

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        if (e != null) {
            System.out.println("Error while committing offset" + e.getMessage());
        } else {
            System.out.println("Committed offsets");
            System.out.println(map.entrySet());
        }
    }
}
