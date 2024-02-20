package com.example.kafka.service.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListenerExampleImpl {

    @KafkaListener(topics = "topic2")
    public void topic2Listener(ConsumerRecord<String, String> record) {
        System.out.println(record.partition());
        System.out.println(record.key());
        System.out.println(record.value());
    }

    @KafkaListener(topics = "topic3")
    public void topic3Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic3");
        System.out.println(record.partition());
        System.out.println(record.key());
        System.out.println(record.value());
    }
}
