/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bp.kafka.kstream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author t821012
 */
public final class Utils {
    private static String propFile;
    private static Logger LOGGER = LogManager.getLogger(Utils.class);
    private static Properties props = new Properties();

    /*
        Constructor for test and dev
    */
    public Utils() {
    }    

    /*
        Constructor to read properties from prop file.
    */
    
    public Utils(String propFile){
        this.propFile = propFile;
        LOGGER.info("reading properties from file: {}", propFile);
        try {
            InputStream input = new FileInputStream(propFile);
            props.load(input);
            props.forEach((key, value) -> LOGGER.info(key + ":" + value));

        } catch (FileNotFoundException e1) {
            // Log for property file not found.            
            //e1.printStackTrace();
            LOGGER.error("property file {} not found", propFile);
        } catch (IOException e) {
            // Log for property file not loaded.
            LOGGER.error(e);
        }        
    }    
    public static Properties getTestProperties(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "0.0.0.0:8000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "virdevserver:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put("input.topic", "input-topic");
        props.put("filter.topic","filter-topic");
        props.put("filtered.topic", "outgoing-topic");
        props.put("message.fields","deviceName,indicatorName,deviceIp,objectName,objectDesc,time,value");
        props.put("key.fields","deviceName,indicatorName");
        return props;
    }
    public static Properties getDevProperties(){
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "0.0.0.0:8000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "virdevserver:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put("input.topic", "input-topic");
        props.put("filter.topic","filter-topic");
        props.put("filtered.topic", "outgoing-topic");
        props.put("message.fields","deviceName,indicatorName,deviceIp,objectName,objectDesc,time,value");
        props.put("key.fields","deviceName,indicatorName");
        return props;
    }
    
    public static Properties getProperties(){
        return props;        
    }
    
}
