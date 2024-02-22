package com.example.kafka.service.impl;

import com.example.kafka.service.KafkaProducerExample;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaProducerExampleImpl implements KafkaProducerExample {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaTemplate<String, String> customPartitionKafkaTemplate;

    @Autowired
    public KafkaProducerExampleImpl(@Qualifier("kafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
                                    @Qualifier("customPartitionKafkaTemplate") KafkaTemplate<String, String> customPartitionKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.customPartitionKafkaTemplate = customPartitionKafkaTemplate;
    }

    @Override
    public void sendData(String messageId, String message) {
        // kafkaTemplate.send("topic2", messageId, message);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("topic2", messageId, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }

    @Override
    public void sendDataToPartition(String messageId, String message) {
        // kafkaTemplate.send("topic1", messageId, message);
        CompletableFuture<SendResult<String, String>> future = customPartitionKafkaTemplate.send("topic1", messageId, message);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        message + "] due to : " + ex.getMessage());
            }
        });
    }
}
