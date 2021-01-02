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

package com.nightmare.producers;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class BasicPartitionExample {

    public static void main(String[] args) {


        try {
            Boolean syncSend = true;
            String topic = "demotopic";
            Properties producerConfig = new Properties();
            producerConfig.put("bootstrap.servers", "172.17.176.201:9092");
            producerConfig.put("client.id", "basic-producer");
            producerConfig.put("acks", "all");
            producerConfig.put("retries", "3");
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "kafka.examples.producer.CustomPartitioner");

           SimpleProducer<String, String> producer = new SimpleProducer<>(producerConfig, syncSend);


            for (int i = 0; i < 15; i++) {
                System.out.println("Sending message "+(i+1));
                producer.send(topic, i + "", "This is a " + ((i % 2) == 0 ? "Even" : "Odd") + " Message");
            }

            producer.close();
        } catch (Exception e) {
            throw e;
        }

    }

}