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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TransactionalProducerExample {

    public static void main(String[] args) throws Exception {
        Producer<String, String> producer = null;

        try {

            String topic = "simple_transactional_topic";
            Properties producerConfig = new Properties();
            producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.118.116:9092");
            producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "transactional-producer");
            producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // enable idempotence
            producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "demo-transactional-id"); // set transaction id
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

            producer = new KafkaProducer<>(producerConfig);

            producer.initTransactions(); //initiate transactions

            producer.beginTransaction(); //begin transactions
            for (int i = 16; i < 20; i++) {
                System.out.println("Sending message " + i);
                ProducerRecord record = new ProducerRecord<String, String>(topic, i + "", "A message from an idempotent producer");
                producer.send(record).get();
                if (i == 18) {
                    //To test that the consumer doesn't read the messages till the whole batch is committed
                    //Comment this section to let the consumer read the messages.
                  throw new Exception("A self-destructive exception");
                }
            }
            producer.commitTransaction(); //commit

        } catch (Exception e) {
            // For all other exceptions, just abort the transaction and try again.
            System.out.println("Exception Caught");
            producer.abortTransaction();
            throw e;
        } finally {
            producer.close();
        }

    }

}