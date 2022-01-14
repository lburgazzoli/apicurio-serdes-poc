package io.github.lburgazzoli.sr.serdes;

import org.apache.avro.Schema;
import org.apache.kafka.common.header.Headers;

import io.apicurio.registry.serde.avro.AvroEncoding;
import io.apicurio.registry.serde.avro.AvroSerdeHeaders;

public class AvroSerializer extends BaseSerializer<Schema> {
    private final AvroSerdeHeaders avroHeaders = new AvroSerdeHeaders(false);

    public AvroSerializer() {
        super(Avro.SCHEMA_PARSER, new InflightSchemaResolver<>());
    }

    @Override
    protected void configureHeaders(Headers headers) {
        avroHeaders.addEncodingHeader(headers, AvroEncoding.BINARY.name());
    }
}
