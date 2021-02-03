package com.nightmare.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {


    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.118.125:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
               //  .count()
                /**comment below count statement and uncomment above to make this a non-materialized application**/
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
                .toStream()
                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));


        final Topology topology = builder.build();
        System.out.println("This is how the topology looks like");
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        /**Comment this code block to avoid calling the interactive query method if you do not use the materialized view**/
        Thread t = new Thread("query-thread") {
            @Override
            public void run() {
                try {
                    interactiveQuery(streams);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        try {
            streams.start();
            t.start();
            latch.await();

        } catch (Throwable e) {
            System.exit(1);
        }
        // interactiveQuery(streams);
        System.exit(0);

    }

    private static void interactiveQuery(KafkaStreams streams) throws InterruptedException {
        while (true) {
            Thread.sleep(4000);
            System.out.println("Running interactive query");
            System.out.println(streams.state().toString());
            // if (streams.state().isRunning()) {
            try {
                ReadOnlyKeyValueStore<String, Long> keyValueStore =
                        streams.store("counts-store", QueryableStoreTypes.keyValueStore());
                // Get the values for all of the keys available in this application instance
                KeyValueIterator<String, Long> range = keyValueStore.all();
                while (range.hasNext()) {
                    KeyValue<String, Long> next = range.next();
                    System.out.println("count for " + next.key + ": " + next.value);
                }
            } catch (InvalidStateStoreException ex) {

            }
        }
    }
}
