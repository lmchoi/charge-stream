package com.doinkey.cg;

import com.doinkey.cg.app.ChargeService;
import com.doinkey.cg.config.Configuration;
import com.doinkey.cg.config.ConfigurationLoader;
import com.doinkey.cg.config.StreamPropertiesBuilder;

import java.util.Properties;

public class App
{
    public static void main( String[] args ) throws InterruptedException {
        String configFilename = args[0];

        // read config
        ConfigurationLoader configurationLoader = new ConfigurationLoader();
        Configuration config = configurationLoader.load(configFilename);
        Properties chargeStreamProperties = StreamPropertiesBuilder.build(config.getChargeStream());

        // forgive me for my lack of creativity with the naming...
        ChargeService chargeService = new ChargeService(chargeStreamProperties);
        chargeService.start();
    }
}
