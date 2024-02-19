package com.example.main.simple.service.impl;

import com.example.main.simple.service.KafkaProducerSimple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component("producerSimple")
public class KafkaProducerSimpleImpl implements KafkaProducerSimple {

    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public KafkaProducerSimpleImpl(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void sendData(String messageId, String message) {
        kafkaTemplate.send("topic2", messageId, message);
    }
}
