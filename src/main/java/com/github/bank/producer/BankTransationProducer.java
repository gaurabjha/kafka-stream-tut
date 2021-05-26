package com.github.bank.producer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransationProducer {
    public static void main(String[] args) {
        //Create the Kafka Properties
        Properties properties = new Properties();

        //kafka boostrap server
        properties.setProperty("bootstrap_server", "127.0.0.1:9092");
        // Data types for the Key and Value
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");


        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "ture"); //ensure we dont push duplicates

        // Produce the Kafka Producer
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        int i = 0;
        while (true) {
            System.out.println("Producing batch :" + i);
            try {
                producer.send(newRandomTransactions("John"));
                Thread.sleep(100);
                producer.send(newRandomTransactions("Stephane"));
                Thread.sleep(100);
                producer.send(newRandomTransactions("Alice"));
                Thread.sleep(100);
                i++;
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    public static ProducerRecord<String, String> newRandomTransactions(String name) {
        //Crete empty Json
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        //Random Amount between 0 and 100 (excluded)
        Integer amount = ThreadLocalRandom.current().nextInt(0,100);

        // Get Current Time
        Instant now = Instant.now();

        // put the transaction details generated above
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>("bank-transactions", name, transaction.toString());
    }
}
