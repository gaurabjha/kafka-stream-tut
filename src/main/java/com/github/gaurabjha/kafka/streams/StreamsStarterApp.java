package com.github.gaurabjha.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {
        System.out.println("Hello World!");

        //Configure the Properties of the Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        //Create Stream
        KStreamBuilder builder = new KStreamBuilder();
//        1. Stream from Kafka
        KStream<String, String> wordCountInput =  builder.stream("wordcount-input");


//        2. Map values to lower case
        KTable<String, Long> wordCount = wordCountInput.mapValues(value -> value.toLowerCase())
//        3. flataMap values split by space
        .flatMapValues(value -> Arrays.asList(value.split("\\s+")))
//        4. select key to apply a key (we discard the old key)
        .selectKey((key, value) -> value)
//        5. group by key before aggregation.
        .groupByKey()
//        6, count occurances
        .count("Counts");


//        7. to in order to write the results back to kafka
        wordCount.to(Serdes.String(), Serdes.Long(), "word-count-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}