/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bp.kafka.kstream;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

/**
 *
 * @author t821012
 */
public final class TestUtils {
    
    public static Properties getProperties(){
        Properties testProp = new Properties();
        testProp.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        testProp.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "0.0.0.0:8000");
        testProp.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "capltda28:9092");
        testProp.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        testProp.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testProp.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        testProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        testProp.put("input.topic", "sevone-test-topic");
        testProp.put("filter.topic","test-topic");
        testProp.put("filtered.topic", "splunk-test-filtered-topic");
        testProp.put("message.fields","deviceName,indicatorName,deviceIp,objectName,objectDesc,time,value");
        testProp.put("key.fields","deviceName,indicatorName");
        return testProp;
    }
}
