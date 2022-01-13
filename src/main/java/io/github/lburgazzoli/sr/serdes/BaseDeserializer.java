package io.github.lburgazzoli.sr.serdes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.header.Headers;

import io.apicurio.registry.serde.AbstractKafkaDeserializer;
import io.apicurio.registry.serde.ParsedSchema;
import io.apicurio.registry.serde.SchemaParser;
import io.github.lburgazzoli.sr.Constants;

public class BaseDeserializer<S> extends AbstractKafkaDeserializer<S, byte[]> {
    private final SchemaParser<S> parser;

    protected BaseDeserializer(SchemaParser<S> parser) {
        this.parser = parser;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(new BaseConfig(configs), isKey);
    }

    @Override
    public SchemaParser<S> schemaParser() {
        return parser;
    }

    @Override
    protected byte[] readData(ParsedSchema<S> schema, ByteBuffer buffer, int start, int length) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    protected byte[] readData(Headers headers, ParsedSchema<S> schema, ByteBuffer buffer, int start, int length) {
        headers.add(Constants.SCHEMA_HEADER, schema.getParsedSchema().toString().getBytes(StandardCharsets.UTF_8));

        byte[] msgData = new byte[length];
        buffer.get(msgData, start, length);

        return msgData;
    }
}