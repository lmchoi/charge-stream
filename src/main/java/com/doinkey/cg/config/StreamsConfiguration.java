package com.doinkey.cg.config;

import java.util.List;

public class StreamsConfiguration {
    // looks unused by it's actually used by jackson!
    private String applicationId;
    private List<String> bootstrapServers;
    private String schemaRegistryUrl;

    public String getApplicationId() {
        return applicationId;
    }

    public List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }
}
