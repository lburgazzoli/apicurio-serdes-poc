package io.github.lburgazzoli.sr.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.google.gson.annotations.SerializedName;
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

    public static SchemaGenerator SCHEMA_GENERATOR = schemaGenerator();

    private Json() {
    }

    public static SchemaGenerator schemaGenerator() {
        SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(
            new ObjectMapper(),
            SchemaVersion.DRAFT_7,
            OptionPreset.PLAIN_JSON);

        configBuilder.forFields()
            .withPropertyNameOverrideResolver(field -> {
                SerializedName sn = field.getAnnotationConsideringFieldAndGetter(SerializedName.class);
                if (sn != null) {
                    return sn.value();
                }

                return null;
            });

        SchemaGeneratorConfig config = configBuilder.build();

        return new SchemaGenerator(config);
    }
}
