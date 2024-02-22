package com.example.kafka.service;

public interface KafkaProducerExample {

    void sendData(String messageId, String message);

    void sendDataToPartition(String messageId, String message);
}
