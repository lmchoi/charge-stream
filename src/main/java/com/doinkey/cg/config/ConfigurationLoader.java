package com.doinkey.cg.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;

public class ConfigurationLoader {
    private ObjectMapper objectMapper;

    public ConfigurationLoader() {
        objectMapper = new ObjectMapper(new YAMLFactory());
    }

    public Configuration load(String configFilename) {
        try {
            return objectMapper.readValue(new File(configFilename), Configuration.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse configure file: " + configFilename, e);
        }
    }
}
