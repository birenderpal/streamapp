/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spark.kafka.kstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author t821012
 */
public class SplunkStream {

    private static String propFile;
    private static String APPLICATION_SERVER_CONFIG;
    private static Logger LOGGER = LogManager.getLogger(SplunkStream.class);
    private static Properties props = new Properties();
    private static String APPLICATION_ID_CONFIG;
    private static String BOOTSTRAP_SERVERS_CONFIG;
    private static String INPUT_TOPIC;
    static String INV_STORE_NAME = System.getProperty("sevone.store", "sevone-store");
    static String IND_STORE_NAME = System.getProperty("indicator.store", "indicator-store");
    static KafkaStreams splunkStream;

    public SplunkStream(String propFile, String appServer) {
        this.propFile = propFile;
        this.APPLICATION_SERVER_CONFIG = appServer;
        this.props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, this.APPLICATION_SERVER_CONFIG);
    }

    public SplunkStream(String propFile, String appID, String brokers, String inputTopic) {
        this.propFile = propFile;
        this.APPLICATION_ID_CONFIG = appID;
        this.INPUT_TOPIC = inputTopic;
        this.BOOTSTRAP_SERVERS_CONFIG = brokers;
    }

    private static void readProps() {
        //InputStream input = null;
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

    public KafkaStreams createSplunkStream() {
        readProps();
        final ObjectMapper MAPPER = new ObjectMapper();
        String SPLUNK_INVENTORY_TOPIC = props.getProperty("splunk.inventory.topic");
        String SPLUNK_OUTPUT_TOPIC = props.getProperty("splunk.filtered.topic");
        INPUT_TOPIC = props.getProperty("input.topic");
        LOGGER.info("Building Stream with {}", INPUT_TOPIC);
        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, String> invstream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((String value) -> {
                    try {
                        JsonNode json = MAPPER.readTree(value);
                        ObjectNode returnJson = MAPPER.createObjectNode();
                        returnJson.put("deviceName", json.get("deviceName"));
                        returnJson.put("indicatorName", json.get("indicatorName"));
                        //returnJson.put("format", json.get("format"));                        
                        return MAPPER.readTree(returnJson.toString());
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        LOGGER.error(e);
                        return null;
                    }
                })
                .map((key, value) -> KeyValue.pair(value.get("deviceName").textValue() + value.get("indicatorName").textValue(), value.toString()))
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                    return newValue;
                }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(INV_STORE_NAME).withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        KStream<String, String> sevonestream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((String value) -> {
                    try {
                        JsonNode json = MAPPER.readTree(value);
                        ObjectNode returnJson = MAPPER.createObjectNode();
                        returnJson.put("deviceName", json.get("deviceName"));
                        returnJson.put("indicatorName", json.get("indicatorName"));
                        returnJson.put("objectName", json.get("objectName"));
                        returnJson.put("deviceIp", json.get("deviceIp"));
                        returnJson.put("objectDesc", json.get("objectDesc"));
                        returnJson.put("value", json.get("value"));
                        returnJson.put("time", json.get("time"));
                        //returnJson.put("format", json.get("format"));                        
                        return MAPPER.readTree(returnJson.toString());
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        LOGGER.error(e);
                        return null;
                    }
                })
                .map((key, value) -> KeyValue.pair(value.get("deviceName").textValue() + value.get("indicatorName").textValue(), value.toString()));

        KTable<String, String> inventorytable = builder.stream(SPLUNK_INVENTORY_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                //KTable<String, String> inventorytable = builder.table(SPLUNK_INVENTORY_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((String value) -> {
                    try {
                        JsonNode json = MAPPER.readTree(value);
                        return json;
                    } catch (Exception e) {
                        LOGGER.error(e);
                        return null;
                    }
                })
                .filter((k, v) -> v.get("toSplunk").textValue().equals("true"))
                .mapValues((k, v) -> {
                    //System.out.println(v);
                    return v.toString();
                })
                .filter((k, v) -> v != null)
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                    return newValue;
                }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(IND_STORE_NAME).withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        inventorytable.toStream().peek((key, value) -> LOGGER.debug("New data into table key = {} value ={}", key, value))
                .filter((k, v) -> {
                    try {
                        JsonNode value = MAPPER.readTree(v);
                        if (!(value.get("toSplunk").textValue().equalsIgnoreCase("true"))) {
                            return true;
                        } else {
                            return false;
                        }
                    } catch (IOException ex) {
                        //Logger.getLogger(StreamApp.class.getName()).log(Level.SEVERE, null, ex);
                        LOGGER.error(ex);
                        return false;
                    }

                })
                .mapValues((k, v) -> null)
                .to(SPLUNK_INVENTORY_TOPIC);

        KStream<String, String> tosplunk = sevonestream.join(inventorytable, (String sevone, String inv) -> sevone,
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );
        tosplunk.to(SPLUNK_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        final Topology topology = builder.build();
        splunkStream = new KafkaStreams(topology, props);

        LOGGER.trace(splunkStream.toString());
        splunkStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            // add logger
            LOGGER.error("splunkStream Uncaught exception in Thread {0} - {1}", new Object[]{thread, throwable.getMessage()});
            throwable.printStackTrace();

        });
        splunkStream.start();
        return splunkStream;
    }

    public static <T> T waitUntilStoreIsQueryable(final String storeName,
            final QueryableStoreType<T> queryableStoreType,
            final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                return streams.store(storeName, queryableStoreType);
            } catch (final InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(50);
            }
        }
    }
}
