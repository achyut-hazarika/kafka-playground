package com.nightmare.consumers;

import com.nightmare.utils.RebalanceListenerImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerWithRebalancer {

    public static void main(String[] args) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 3; i++) {
            int finalI = i;
            executorService.execute(() -> {
                try {
                    startConsumer("consumer-" + finalI, "partitioned_topic");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            Thread.sleep(5000);
        }

        executorService.shutdown();
        executorService.awaitTermination(3, TimeUnit.MINUTES);
    }

    private static void startConsumer(String name, String topic) throws InterruptedException {

        Properties consumerConfig = new Properties();
        consumerConfig.put("group.id", "demo-group");
        consumerConfig.put("bootstrap.servers", "192.168.118.120:9092");
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
        RebalanceListenerImpl rebalanceListener = new RebalanceListenerImpl(consumer,name);

        consumer.subscribe(Collections.singletonList(topic), rebalanceListener);

        System.out.printf("starting consumerName: %s%n", name);

        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
         /*   if (records == null || records.isEmpty()) {
                System.out.println("No new messages to read from.");

            }*/
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumer [%s] Received Message topic =%s, partition =%s, offset = %d, key = %s, value = %s\n", name, record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    rebalanceListener.addOffset(record.topic(), record.partition(), record.offset());
                    //Thread.sleep(2000);
                }

            } catch (Exception e) {
                // System.out.println("Committed");
                System.out.printf("closing consumerName: %s%n", name);
                consumer.close();
            }

            //Thread.sleep(2000);
        }
    }
}
