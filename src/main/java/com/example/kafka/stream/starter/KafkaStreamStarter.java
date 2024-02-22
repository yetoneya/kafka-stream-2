package com.example.kafka.stream.starter;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import java.util.Properties;

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
        runAfterStartup1();
        runAfterStartup2();
    }

    private void runAfterStartup1() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("topic2", Consumed.with(STRING_SERDE, STRING_SERDE))
                .map((k, v) -> KeyValue.pair(k, v != null ? v.toUpperCase() : "NULL"))
                .to("topic3");
        final Topology topology = builder.build();
        Properties props = kStreamsConfig.asProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "APP1");
        final KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook1") {
            @Override
            public void run() {
                streams.close();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
        } catch (Throwable e) {
        }
    }

    private void runAfterStartup2() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("topic1", Consumed.with(STRING_SERDE, STRING_SERDE))
                .map((k, v) -> KeyValue.pair(k, v != null ? v.repeat(2) : "NULL"))
                .to("topic4", Produced.with(STRING_SERDE, STRING_SERDE,
                        (topic, key, value, numPartitions) -> key.endsWith("0") ? 0 : key.endsWith("1") ? 1 : 2));
        final Topology topology = builder.build();
        Properties props = kStreamsConfig.asProperties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "APP2");
        final KafkaStreams streams = new KafkaStreams(topology, props);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook2") {
            @Override
            public void run() {
                streams.close();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
        } catch (Throwable e) {
        }
    }
}
