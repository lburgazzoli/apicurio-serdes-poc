package io.github.lburgazzoli.sr.serdes;

import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;

import io.apicurio.registry.serde.ParsedSchema;
import io.github.lburgazzoli.sr.Constants;

public class AvroDeserializer extends BaseDeserializer<Schema> {
    public AvroDeserializer() {
        super(Avro.SCHEMA_PARSER);
    }

    @Override
    protected void configureHeaders(Headers headers, ParsedSchema<Schema> schema) {
        headers.add(
            Constants.SCHEMA_HEADER,
            schema.getParsedSchema().toString().getBytes(StandardCharsets.UTF_8));
    }
}
