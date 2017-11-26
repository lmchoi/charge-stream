package com.doinkey.cg.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;

public class Topic<K, V> {
    private final String name;
    private final Serde keySerde;
    private final Serde valueSerde;

    public Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public void send(KStream<K, V> stream) {
        stream.to(keySerde, valueSerde, name);
    }

    public String getName() {
        return name;
    }
}
