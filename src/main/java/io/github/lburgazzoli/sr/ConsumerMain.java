package io.github.lburgazzoli.sr;


import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.utils.IoUtil;
import io.github.lburgazzoli.sr.model.Greeting;
import io.github.lburgazzoli.sr.serdes.JsonDeserializer;

public class ConsumerMain extends Constants {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String [] args) throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        Properties props = new Properties();
        props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "c-" + TOPIC_NAME);
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.putIfAbsent(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.putIfAbsent(SerdeConfig.REGISTRY_URL, REGISTRY_URL);

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            LOGGER.info("Subscribing to topic {}", TOPIC_NAME);

            ObjectMapper mapper = new ObjectMapper();

            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            while(running.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
                if (records.count() == 0) {
                    LOGGER.info("No messages waiting...");
                } else {
                    records.forEach(record -> {
                        Greeting payload;

                        try {
                            payload = mapper.readerFor(Greeting.class).readValue(record.value());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        LOGGER.info("Consumed a message: {} headers: {}",
                            payload,
                            StreamSupport.stream(record.headers().spliterator(), false)
                                .collect(Collectors.toMap(
                                    Header::key,
                                    h -> IoUtil.toString(h.value())
                                )));
                    });
                }
            }
        }
    }
}