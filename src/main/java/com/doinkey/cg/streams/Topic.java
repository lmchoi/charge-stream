package com.doinkey.cg.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class Topic<K, V> {
    private final String name;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;

    public Topic(String name, Serde<K> keySerde, Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }
    
    public KStream<K, V> stream(KStreamBuilder builder) {
        return builder.stream(keySerde, valueSerde, name);
    }

    public void send(KStream<K, V> stream) {
        stream.to(keySerde, valueSerde, name);
    }

    public String getName() {
        return name;
    }
}
