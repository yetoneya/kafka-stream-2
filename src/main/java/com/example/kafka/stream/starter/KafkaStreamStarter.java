package com.example.kafka.stream.starter;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

@Component
public class KafkaStreamStarter {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private final KafkaStreamsConfiguration kStreamsConfig;

    @Autowired
    public KafkaStreamStarter(KafkaStreamsConfiguration kStreamsConfig) {
        this.kStreamsConfig = kStreamsConfig;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void runAfterStartup() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("topic2", Consumed.with(STRING_SERDE, STRING_SERDE))
                .map((k, v) -> KeyValue.pair(k, v != null ? v.toUpperCase() : "NULL"))
                .to("topic3");
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, kStreamsConfig.asProperties());
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
            }
        });

        try {
            streams.start();
        } catch (Throwable e) {
        }
    }
}
