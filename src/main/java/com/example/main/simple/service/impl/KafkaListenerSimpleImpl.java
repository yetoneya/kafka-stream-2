package com.example.main.simple.service.impl;

import com.example.main.simple.service.KafkaListenerSimple;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component("listenerSimple")
public class KafkaListenerSimpleImpl implements KafkaListenerSimple {

    @Override
    @KafkaListener(topics = "topic2")
    public void messageListener(String message) {
        System.out.println(message);
    }
}
