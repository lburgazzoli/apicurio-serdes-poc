package io.github.lburgazzoli.sr.serdes;

import org.apache.avro.Schema;

public class AvroDeserializer extends BaseDeserializer<Schema> {
    public AvroDeserializer() {
        super(Avro.SCHEMA_PARSER);
    }
}
