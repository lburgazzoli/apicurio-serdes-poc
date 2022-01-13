package io.github.lburgazzoli.sr.serdes;

import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;

import io.apicurio.registry.serde.SchemaParser;
import io.apicurio.registry.types.ArtifactType;
import io.apicurio.registry.utils.IoUtil;

public final class Json {
    public static JsonSchemaFactory SCHEMA_FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);

    public static SchemaParser<JsonSchema> SCHEMA_PARSER = new SchemaParser<>() {
        @Override
        public ArtifactType artifactType() {
            return ArtifactType.JSON;
        }

        @Override
        public JsonSchema parseSchema(byte[] rawSchema) {
            return SCHEMA_FACTORY.getSchema(IoUtil.toString(rawSchema));
        }
    };

    private Json() {
    }
}
