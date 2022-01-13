package io.github.lburgazzoli.sr;


import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.apicurio.registry.serde.SerdeConfig;
import io.github.lburgazzoli.sr.model.Greeting;
import io.github.lburgazzoli.sr.serdes.Json;
import io.github.lburgazzoli.sr.serdes.JsonSerializer;

public class ProducerMain extends Constants {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String [] args) throws Exception {
        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        Properties props = new Properties();
        props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        props.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "p-" + TOPIC_NAME);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.putIfAbsent(SerdeConfig.REGISTRY_URL, REGISTRY_URL);
        props.putIfAbsent(SerdeConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE);

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            ObjectMapper mapper = new ObjectMapper();
            String schema = Json.SCHEMA_GENERATOR.generateSchema(Greeting.class).toString();

            LOGGER.info("Publish to topic {}", TOPIC_NAME);
            LOGGER.info("Computed schema: {}", schema);

            for (int i = 0; running.get(); i++) {
                Greeting payload = new Greeting();
                payload.setMessage("" + i );
                payload.setTime("" + new Date(System.currentTimeMillis()));

                LOGGER.info("Sending {}", payload);

                ProducerRecord<String, byte[]> producedRecord = new ProducerRecord<>(
                    TOPIC_NAME,
                    "foo",
                    mapper.writer().writeValueAsBytes(payload));

                producedRecord.headers().add(Constants.SCHEMA_HEADER, schema.getBytes(StandardCharsets.UTF_8));
                producer.send(producedRecord);

                Thread.sleep(1000);
            }
        }
    }

}
