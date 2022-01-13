package io.github.lburgazzoli.sr.serdes;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import org.apache.kafka.common.header.Headers;

import io.apicurio.registry.serde.AbstractKafkaSerializer;
import io.apicurio.registry.serde.ParsedSchema;
import io.apicurio.registry.serde.SchemaParser;
import io.github.lburgazzoli.sr.Constants;

public abstract class BaseSerializer<S> extends AbstractKafkaSerializer<S, byte[]> {
    private final SchemaParser<S> parser;

    public BaseSerializer(SchemaParser<S> parser) {
        this.parser = parser;
    }

    @Override
    public SchemaParser<S> schemaParser() {
        return parser;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(new BaseConfig(configs), isKey);
    }

    @Override
    protected void serializeData(ParsedSchema<S> schema, byte[] data, OutputStream out) throws IOException {
        serializeData(null, schema, data, out);
    }

    @Override
    protected void serializeData(Headers headers, ParsedSchema<S> schema, byte[] data, OutputStream out) throws IOException {
        if (headers != null) {
            configureHeaders(headers);
        }

        out.write(data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, byte[] data) {
        if (data == null) {
            return null;
        }
        if (headers == null) {
            throw new IllegalStateException("headers are required");
        }
        if (headers.lastHeader(Constants.SCHEMA_HEADER) == null) {
            throw new IllegalStateException("schema header is required");
        }

        try {
            return super.serialize(topic, headers, data);
        } finally {
            headers.remove(Constants.SCHEMA_HEADER);
        }
    }

    protected abstract void configureHeaders(Headers headers);
}
