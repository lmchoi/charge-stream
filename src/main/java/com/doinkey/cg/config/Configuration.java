package com.doinkey.cg.config;

public class Configuration {
    // looks unused by it's actually used by jackson!
    private StreamsConfiguration chargeStream;

    public StreamsConfiguration getChargeStream() {
        return chargeStream;
    }
}
