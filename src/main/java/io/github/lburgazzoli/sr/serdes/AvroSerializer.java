package io.github.lburgazzoli.sr.serdes;

import org.apache.avro.Schema;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import io.apicurio.registry.serde.ParsedSchema;
import io.apicurio.registry.serde.ParsedSchemaImpl;
import io.apicurio.registry.serde.avro.AvroEncoding;
import io.apicurio.registry.serde.avro.AvroSerdeHeaders;
import io.apicurio.registry.utils.IoUtil;
import io.github.lburgazzoli.sr.Constants;

public class AvroSerializer extends BaseSerializer<Schema> {
    private final AvroSerdeHeaders avroHeaders = new AvroSerdeHeaders(false);

    public AvroSerializer() {
        super(Avro.SCHEMA_PARSER);
    }

    @Override
    protected ParsedSchema<Schema> getSchemaFromData(Headers headers, byte[] data) {
        Header schemaHeader = headers.lastHeader(Constants.SCHEMA_HEADER);
        Schema schema = schemaParser().parseSchema(schemaHeader.value());

        return new ParsedSchemaImpl<Schema>()
            .setParsedSchema(schema)
            .setRawSchema(IoUtil.toBytes(schema.toString()));
    }

    @Override
    protected void configureHeaders(Headers headers) {
        avroHeaders.addEncodingHeader(headers, AvroEncoding.BINARY.name());
    }
}
