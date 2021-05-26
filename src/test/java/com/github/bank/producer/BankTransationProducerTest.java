package com.github.bank.producer;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class BankTransationProducerTest {

    @Test
    public void newRandomTransactionTest(){
        ProducerRecord<String, String> record = BankTransationProducer.newRandomTransactions("John");
        String key = record.key();
        String value = record.value();

        Assert.assertEquals(key, "John");
        System.out.println(value);

        ObjectMapper mapper = new ObjectMapper();
        try{
            JsonNode node = mapper.readTree(value);
            Assert.assertEquals(node.get("name").asText(), "John");
            Assert.assertTrue("Amount should be less than 100", node.get("amount").asInt() < 100 );

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}