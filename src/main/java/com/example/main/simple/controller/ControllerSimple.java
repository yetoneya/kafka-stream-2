package com.example.main.simple.controller;

import com.example.main.simple.service.KafkaProducerSimple;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("simple")
public class ControllerSimple {

    private final KafkaProducerSimple kafkaProducerSimple;

    @Autowired
    public ControllerSimple(@Qualifier("producerSimple") KafkaProducerSimple kafkaProducerSimple) {
        this.kafkaProducerSimple = kafkaProducerSimple;
    }

    @PostMapping
    public void sendData(String messageId, String message) {
        kafkaProducerSimple.sendData(messageId, message);
    }
}
