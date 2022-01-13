package io.github.lburgazzoli.kdf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import io.apicurio.registry.serde.AbstractKafkaDeserializer;
import io.apicurio.registry.serde.AbstractKafkaSerializer;
import io.apicurio.registry.serde.ParsedSchema;
import io.apicurio.registry.serde.ParsedSchemaImpl;
import io.apicurio.registry.serde.SchemaParser;
import io.apicurio.registry.serde.avro.AvroEncoding;
import io.apicurio.registry.serde.avro.AvroSchemaParser;
import io.apicurio.registry.serde.avro.AvroSerdeHeaders;
import io.apicurio.registry.serde.config.BaseKafkaSerDeConfig;
import io.apicurio.registry.utils.IoUtil;

public class SerDes {
    public static final String SCHEMA_HEADER = "payload.schema";
    public static final String SERVERS = "localhost:9092";
    public static final String TOPIC_NAME = "test";
    public static final String REGISTRY_URL = "http://localhost:8080/apis/registry/v2";

    /**
     * Avro serializer that accept bytes and
     */
    public static class AvroSerializer extends AbstractKafkaSerializer<Schema, byte[]> {
        private final AvroSchemaParser parser = new AvroSchemaParser();
        private final AvroSerdeHeaders avroHeaders = new AvroSerdeHeaders(false);

        @Override
        public SchemaParser<Schema> schemaParser() {
            return parser;
        }

        @Override
        protected ParsedSchema<Schema> getSchemaFromData(Headers headers, byte[] data) {
            Header schemaHeader = headers.lastHeader(SCHEMA_HEADER);
            Schema schema = new Schema.Parser().parse(IoUtil.toString(schemaHeader.value()));

            return new ParsedSchemaImpl<Schema>()
                .setParsedSchema(schema)
                .setRawSchema(IoUtil.toBytes(schema.toString()));
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            super.configure(new AvroSerdeConfig(configs), isKey);
        }

        @Override
        protected void serializeData(ParsedSchema<Schema> schema, byte[] data, OutputStream out) throws IOException {
            out.write(data);
        }

        @Override
        protected void serializeData(Headers headers, ParsedSchema<Schema> schema, byte[] data, OutputStream out) throws IOException {
            if (headers != null) {
                avroHeaders.addEncodingHeader(headers, AvroEncoding.BINARY.name());
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
            if (headers.lastHeader(SCHEMA_HEADER) == null) {
                throw new IllegalStateException("schema header is required");
            }

            try {
                return super.serialize(topic, headers, data);
            } finally {
                headers.remove(SCHEMA_HEADER);
            }
        }
    }

    public static class AvroDeserializer extends AbstractKafkaDeserializer<Schema, byte[]> {
        private final AvroSchemaParser parser = new AvroSchemaParser();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            super.configure(new AvroSerdeConfig(configs), isKey);
        }

        @Override
        public SchemaParser<Schema> schemaParser() {
            return parser;
        }

        @Override
        protected byte[] readData(ParsedSchema<Schema> schema, ByteBuffer buffer, int start, int length) {
            throw new UnsupportedOperationException("Unsupported");
        }

        @Override
        protected byte[] readData(Headers headers, ParsedSchema<Schema> schema, ByteBuffer buffer, int start, int length) {
            headers.add(SCHEMA_HEADER, schema.getParsedSchema().toString().getBytes(StandardCharsets.UTF_8));

            byte[] msgData = new byte[length];
            buffer.get(msgData, start, length);

            return msgData;
        }
    }

    public static class AvroSerdeConfig extends BaseKafkaSerDeConfig {
        public AvroSerdeConfig(Map<?, ?> originals) {
            super(originals);
        }
    }
}
