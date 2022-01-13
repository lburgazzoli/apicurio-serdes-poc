package io.github.lburgazzoli.sr.serdes;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.header.Headers;

import com.networknt.schema.JsonSchema;

import io.apicurio.registry.serde.ParsedSchema;
import io.github.lburgazzoli.sr.Constants;

public class JsonDeserializer extends BaseDeserializer<JsonSchema> {
    public JsonDeserializer() {
        super(Json.SCHEMA_PARSER);
    }

    @Override
    protected void configureHeaders(Headers headers, ParsedSchema<JsonSchema> schema) {
        headers.add(
            Constants.SCHEMA_HEADER,
            schema.getParsedSchema().getSchemaNode().toString().getBytes(StandardCharsets.UTF_8));
    }
}
