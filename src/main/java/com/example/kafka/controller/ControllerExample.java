package com.example.kafka.controller;

import com.example.kafka.service.KafkaProducerExample;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class ControllerExample {

    private final KafkaProducerExample kafkaProducerExample;

    @Autowired
    public ControllerExample(KafkaProducerExample kafkaProducerConfigured) {
        this.kafkaProducerExample = kafkaProducerConfigured;
    }

    @PostMapping("/example")
    public void sendData(String messageId, String message) {
        kafkaProducerExample.sendData(messageId, message);
    }

    @PostMapping("/partition")
    public void sendDataToPartition(String messageId, String message) {
        kafkaProducerExample.sendDataToPartition(messageId, message);
    }
}
