package io.github.lburgazzoli.sr.serdes;

import org.apache.avro.Schema;

import io.apicurio.registry.serde.SchemaParser;
import io.apicurio.registry.serde.avro.AvroSchemaParser;

public final class Avro {
    public static SchemaParser<Schema> SCHEMA_PARSER = new AvroSchemaParser();

    private Avro() {
    }
}
