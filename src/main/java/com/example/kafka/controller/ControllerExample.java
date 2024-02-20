package com.example.kafka.controller;

import com.example.kafka.service.KafkaProducerExample;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("example")
public class ControllerExample {

    private final KafkaProducerExample kafkaProducerConfigured;

    @Autowired
    public ControllerExample(KafkaProducerExample kafkaProducerConfigured) {
        this.kafkaProducerConfigured = kafkaProducerConfigured;
    }

    @PostMapping
    public void sendData(String messageId, String message) {
       kafkaProducerConfigured.sendData(messageId, message);
    }
}
