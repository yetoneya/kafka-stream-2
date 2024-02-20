package com.example.kafka.service.impl;

import com.example.kafka.service.KafkaProducerExample;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaProducerExampleImpl implements KafkaProducerExample {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaProducerExampleImpl(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void sendData(String messageId, String message) {
        // kafkaTemplate.send("topic2", messageId, message);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("topic2", messageId, message);
        try {
            System.out.println(future.handle((success, error) -> success != null ? success : error.getMessage()).get());
            //SendResult [producerRecord=ProducerRecord(topic=topic2, partition=null, headers=RecordHeaders(headers = [], isReadOnly = true), key=1, value=catch another 10, timestamp=null), recordMetadata=topic2-0@14]
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        kafkaTemplate.flush();
    }
}
