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
 * @author Birender Pal
 */
public class FilterStream {

    private static String propFile;
    private static String APPLICATION_SERVER_CONFIG;
    private static Logger LOGGER = LogManager.getLogger(FilterStream.class);
    private static Properties props = new Properties();
    private static String APPLICATION_ID_CONFIG;
    private static String BOOTSTRAP_SERVERS_CONFIG;
    private static String INPUT_TOPIC;
    static String INCOMING_STORE_NAME = System.getProperty("incoming.store", "incoming-store");
    static String OUTGOING_STORE_NAME = System.getProperty("outgoing.store", "filter-store");
    static KafkaStreams filterStream;

    public FilterStream() {
        TestUtils test = new TestUtils();
        props = test.getProperties();
    }

    public FilterStream(String propFile, String appServer) {
        this.propFile = propFile;
        this.APPLICATION_SERVER_CONFIG = appServer;
        readProps();
        this.props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, this.APPLICATION_SERVER_CONFIG);
    }

    public FilterStream(String propFile, String appID, String brokers, String inputTopic) {
        this.propFile = propFile;
        readProps();
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

    public KafkaStreams createFilterStream() {
        //readProps();        
        final ObjectMapper MAPPER = new ObjectMapper();
        String FILTER_TOPIC = props.getProperty("filter.topic", "splunk-ggi-filter-list");
        String OUTPUT_TOPIC = props.getProperty("filtered.topic", "splunk-filtered-topic");
        INPUT_TOPIC = props.getProperty("input.topic");
        LOGGER.info("Building Stream with {}", INPUT_TOPIC);
        StreamsBuilder builder = new StreamsBuilder();

//        KTable<String, String> invtable = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
//                .mapValues((String value) -> {
//                    try {
//                        JsonNode json = MAPPER.readTree(value);
//                        ObjectNode returnJson = MAPPER.createObjectNode();
//                        returnJson.put("deviceName", json.get("deviceName"));
//                        returnJson.put("indicatorName", json.get("indicatorName"));
//                        //returnJson.put("format", json.get("format"));                        
//                        return MAPPER.readTree(returnJson.toString());
//                    } catch (Exception e) {
//                        // TODO Auto-generated catch block
//                        LOGGER.error(e);
//                        return null;
//                    }
//                })
//                .map((key, value) -> KeyValue.pair(value.get("deviceName").textValue() + value.get("indicatorName").textValue(), value.toString()))
//                .groupByKey()
//                .reduce((aggValue, newValue) -> {
//                    return newValue;
//                }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(INCOMING_STORE_NAME).withKeySerde(Serdes.String())
//                        .withValueSerde(Serdes.String()));
        /*
            Read the incoming topic messages into a kafka stream and then
            * Map values to remove un-wanted fields from the message
            * change the key to filter-topic key.
         */
        KStream<String, String> incoming_stream = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
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
                        return returnJson;
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        System.out.println(value);
                        System.out.println(e);
                        LOGGER.error(e);
                        return null;
                    }
                })
                .map((key, value) -> KeyValue.pair(value.get("deviceName").textValue() + value.get("indicatorName").textValue(), value.toString()));

        /*
           Convert the incoming topic stream to a KTable and materialize to a store
            to reduce the size of the store just use the minial fields (the ones used for key)
         */
        KTable<String, String> incomingStreamToTable = incoming_stream
                .mapValues((String value) -> {
                    try {
                        JsonNode json = MAPPER.readTree(value);
                        ObjectNode returnJson = MAPPER.createObjectNode();
                        returnJson.put("deviceName", json.get("deviceName"));
                        returnJson.put("indicatorName", json.get("indicatorName"));
                        //returnJson.put("format", json.get("format"));                        
                        return returnJson.toString();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        LOGGER.error(e);
                        return null;
                    }
                })
                .groupByKey()
                .reduce((aggValue, newValue) -> {
                    return newValue;
                }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(INCOMING_STORE_NAME).withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        /*
            Build a KTable from the filter-topic materialized as Store.
         */
        KTable<String, String> filtertable = builder.table(FILTER_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(OUTGOING_STORE_NAME).withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()))
                //KTable<String, String> inventorytable = builder.table(FILTER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                //.filter((k, v) -> v != null)                
                .mapValues((String value) -> {
                    try {
                        JsonNode json = MAPPER.readTree(value);
                        return json;
                    } catch (Exception e) {
                        LOGGER.error(e);
                        return null;
                    }
                })
                .mapValues((k, v) -> {
                    System.out.println(v);
                    return v.toString();
                });

        /*
            To Keep the store size minimal all disabled messages are deleted from the KTable, this is done by sending a null
            value message to filter-topic
         */
        filtertable.toStream().peek((key, value) -> LOGGER.debug("New data into table key = " + key + " value =" + value))
                .filter((k, v) -> {
                    try {
                        JsonNode value = MAPPER.readTree(v);
                        if (!(value.get("enable").asBoolean())) {
                            return true;
                        } else {
                            return false;
                        }
                    } catch (Exception ex) {
                        //Logger.getLogger(StreamApp.class.getName()).log(Level.SEVERE, null, ex);
                        LOGGER.error(ex);
                        return false;
                    }
                })
                .mapValues((k, v) -> null)
                .to(FILTER_TOPIC);

        /*
            Filter the incoming stream by joining with the KT
         */
        KStream<String, String> filtered = incoming_stream.join(filtertable, (String incoming, String filter) -> incoming,
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        /*
            Write the filtered stream to a topic        
         */
        filtered.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        final Topology topology = builder.build();

        filterStream = new KafkaStreams(topology, props);

        LOGGER.trace(filterStream.toString());

        filterStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            // add logger
            LOGGER.error("filterStream Uncaught exception in Thread {0} - {1}", new Object[]{thread, throwable.getMessage()});
            throwable.printStackTrace();

        });
        filterStream.start();
        System.out.println("Stream started");
        return filterStream;
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
