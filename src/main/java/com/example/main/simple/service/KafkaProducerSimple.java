package com.example.main.simple.service;

public interface KafkaProducerSimple {
    void sendData(String messageId, String message);
}
