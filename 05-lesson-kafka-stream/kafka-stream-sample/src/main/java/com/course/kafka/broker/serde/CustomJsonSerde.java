package com.course.kafka.broker.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class CustomJsonSerde<T> implements Serde<T> {

    private CustomJsonSerializer<T> serializer;
    private CustomJsonDeserializer<T> deserializer;

    // create constructor using all fields
    public CustomJsonSerde(CustomJsonSerializer<T> serializer, CustomJsonDeserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

}
