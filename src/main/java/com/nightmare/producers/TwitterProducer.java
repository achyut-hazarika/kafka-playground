package com.nightmare.producers;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    private static final String CONSUMER_KEY = "rPtpOpvu3n9usXbHF6dHP4jFV";
    private static final String CONSUMER_SECRET_KEY = "9UW6Zhbxnn9XmHvIeoOpLk5ex8qybwxkTyHVRn3B5LYJ5EXLLd";
    private static final String TOKEN = "1037342339834896385-eSjGxYsKIKc6Pv80QsxlM9PDH0MlwH";
    private static final String TOKEN_SECRET = "PktsQujGa7l1FwqA3sV5OktnHt3d30WclKNbQ5qr1AfZy";

    private static final String HASHTAG = "TESLA";

    public static void main(String[] args) {
        Boolean syncSend = true;
        String topic = "twitter-topic";
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "192.168.118.119:9092");
        //producerConfig.put("client.id", "basic-producer");
        producerConfig.put("acks", "all");
        producerConfig.put("retries", "3");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);


        BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Lists.newArrayList(
                HASHTAG));
        Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET_KEY, TOKEN, TOKEN_SECRET);

        Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
                .endpoint(endpoint).authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();


        client.connect();

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            KafkaProducer<Long, String> kafkaProducer = new KafkaProducer<Long, String>(producerConfig);
            while (true) {
                Map<String, Object> jsonMap = objectMapper.readValue(queue.take(),
                        new TypeReference<Map<String, Object>>() {
                        });

                System.out.println("Sending message");
                kafkaProducer.send(new ProducerRecord<Long, String>(topic, String.valueOf(jsonMap.get("text"))));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }


    }
}
