package io.github.lburgazzoli.sr.serdes;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import com.networknt.schema.JsonSchema;

import io.apicurio.registry.serde.ParsedSchema;
import io.apicurio.registry.serde.ParsedSchemaImpl;
import io.apicurio.registry.serde.headers.MessageTypeSerdeHeaders;
import io.github.lburgazzoli.sr.Constants;

public class JsonSerializer extends BaseSerializer<JsonSchema> {
    private MessageTypeSerdeHeaders serdeHeaders;

    public JsonSerializer() {
        super(Json.SCHEMA_PARSER);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);

        serdeHeaders = new MessageTypeSerdeHeaders(new HashMap<>(configs), isKey);
    }

    @Override
    protected ParsedSchema<JsonSchema> getSchemaFromData(Headers headers, byte[] data) {
        Header schemaHeader = headers.lastHeader(Constants.SCHEMA_HEADER);
        JsonSchema schema = schemaParser().parseSchema(schemaHeader.value());

        return new ParsedSchemaImpl<JsonSchema>()
            .setParsedSchema(schema)
            .setRawSchema(schemaHeader.value());
    }

    @Override
    protected void configureHeaders(Headers headers) {
        serdeHeaders.addMessageTypeHeader(headers, byte[].class.getName());
    }
}
