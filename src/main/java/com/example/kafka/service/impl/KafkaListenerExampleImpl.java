package com.example.kafka.service.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
public class KafkaListenerExampleImpl {

    @KafkaListener(topics = "topic2",
            containerFactory = "concurrentKafkaListenerContainerFactory",
            groupId = "group2")
    public void topic2Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic2");
        System.out.println("partition " + record.partition());
        System.out.println("key " + record.key());
        System.out.println(record.value());
    }

    @KafkaListener(topics = "topic3",
            containerFactory = "concurrentKafkaListenerContainerFactory",
            groupId = "group3")
    public void topic3Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic3");
        System.out.println("partition " + record.partition());
        System.out.println("key " + record.key());
        System.out.println(record.value());
    }


    @KafkaListener(topicPartitions = @TopicPartition(topic = "topic1", partitions = {"0"}),
            containerFactory = "concurrentKafkaListenerContainerFactory",
            groupId = "group1")
    public void topic1Partition0Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic1");
        System.out.println("partition " + record.partition());
        System.out.println("key " + record.key());
        System.out.println(record.value());
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "topic1", partitions = {"1"}),
            containerFactory = "concurrentKafkaListenerContainerFactory", groupId = "group1")
    public void topic1Partition1Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic1");
        System.out.println("partition " + record.partition());
        System.out.println("key " + record.key());
        System.out.println(record.value());
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "topic1", partitions = {"2"}),
            containerFactory = "concurrentKafkaListenerContainerFactory",
            groupId = "group1")
    public void topic1Partition2Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic1");
        System.out.println("partition " + record.partition());
        System.out.println("key " + record.key());
        System.out.println(record.value());
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "topic4", partitions = {"0"}),
            containerFactory = "concurrentKafkaListenerContainerFactory",
            groupId = "group4")
    public void topic4Partition0Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic4");
        System.out.println("partition " + record.partition());
        System.out.println("key " + record.key());
        System.out.println(record.value());
    }

    @KafkaListener(topicPartitions = @TopicPartition(topic = "topic4", partitions = {"1"}),
            containerFactory = "concurrentKafkaListenerContainerFactory",
            groupId = "group4")
    public void topic4Partition1Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic4");
        System.out.println("partition " + record.partition());
        System.out.println("key " + record.key());
        System.out.println(record.value());
    }

    @KafkaListener(topicPartitions
            = @TopicPartition(topic = "topic4", partitions = {"2"}),
            containerFactory = "concurrentKafkaListenerContainerFactory",
            groupId = "group4")
    public void topic4Partition2Listener(ConsumerRecord<String, String> record) {
        System.out.println("from topic4");
        System.out.println("partition " + record.partition());
        System.out.println("key " + record.key());
        System.out.println(record.value());
    }
}
