package io.github.lburgazzoli.sr.serdes;

import com.networknt.schema.JsonSchema;

public class JsonDeserializer extends BaseDeserializer<JsonSchema> {
    public JsonDeserializer() {
        super(Json.SCHEMA_PARSER);
    }
}
