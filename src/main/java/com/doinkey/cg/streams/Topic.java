package com.doinkey.cg.streams;

import org.apache.kafka.common.serialization.Serde;

public class Topic {
    private final String name;
    private final Serde keySerde;
    private final Serde valueSerde;

    public Topic(String name, Serde keySerde, Serde valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }
}
