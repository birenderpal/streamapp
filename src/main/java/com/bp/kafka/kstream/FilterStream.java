/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bp.kafka.kstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
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

    private static Logger LOGGER = LogManager.getLogger(FilterStream.class);
    private static Properties props = new Properties();
    private static String INPUT_TOPIC;
    static List messageFields = new ArrayList();
    static List keyFields = new ArrayList();
    static String INCOMING_STORE_NAME = System.getProperty("incoming.store", "incoming-store");
    static String OUTGOING_STORE_NAME = System.getProperty("outgoing.store", "filter-store");
    static KafkaStreams filterStream;

    public FilterStream(Properties props) {
        this.props = props;
        LOGGER.debug("Properties Dump: ", props);
        this.messageFields = Arrays.asList(props.getProperty("message.fields").split(","));
        this.keyFields = Arrays.asList(props.getProperty("key.fields").split(","));
    }

    public KafkaStreams createFilterStream() {
        //readProps();        
        final ObjectMapper MAPPER = new ObjectMapper();
        String FILTER_TOPIC = props.getProperty("filter.topic", "filter-topic");
        String OUTPUT_TOPIC = props.getProperty("filtered.topic", "outgoing-topic");
        INPUT_TOPIC = props.getProperty("input.topic");
        LOGGER.info("Building Stream with {}", INPUT_TOPIC);
        StreamsBuilder builder = new StreamsBuilder();

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
                        Iterator<String> iterator = messageFields.iterator();
                        while (iterator.hasNext()) {
                            String key = iterator.next();
                            returnJson.set(key, json.get(key));                            
                        }
                        return returnJson;
                    } catch (Exception e) {
                        LOGGER.error(e);
                        LOGGER.error("Exception handling value: "+value);
                        ObjectNode returnJson = MAPPER.createObjectNode();
                        Iterator<String> iterator = messageFields.iterator();
                        while (iterator.hasNext()) {
                            String key = iterator.next();
                            returnJson.set(key, null);                            
                        }
                        LOGGER.error("Would return value as: "+returnJson.toString());
                        return returnJson;                        
                    }
                })
                .map((key, value) -> {
                    try {
                        Iterator<String> iterator = keyFields.iterator();
                        String updatedKey = "";
                        while (iterator.hasNext()) {
                            String kkey = iterator.next();
                            updatedKey = updatedKey.concat(value.get(kkey).textValue());
                        }
                        return KeyValue.pair(updatedKey, value.toString());
                    } catch (Exception e) {
                        LOGGER.error("Exception handling key: "+key+" value: "+value);
                        LOGGER.error(e);
                        return KeyValue.pair(key, value.toString());
                    }
                });

        /*
           Convert the incoming topic stream to a KTable and materialize to a store
            to reduce the size of the store just use the minimal fields (the ones used for key)
         */
        KTable<String, String> incomingStreamToTable = incoming_stream
                .mapValues((String value) -> {
                    try {
                        JsonNode json = MAPPER.readTree(value);
                        ObjectNode returnJson = MAPPER.createObjectNode();
                        Iterator<String> iterator = keyFields.iterator();
                        while (iterator.hasNext()) {
                            String key = iterator.next();
                            returnJson.set(key, json.get(key));
                        }
                        return returnJson.toString();
                    } catch (Exception e) {
                        LOGGER.error("Exception handling value: "+value);
                        LOGGER.error(e);
                        return value;
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
                .mapValues((String value) -> {
                    try {
                        JsonNode json = MAPPER.readTree(value);
                        return json;
                    } catch (Exception e) {
                        LOGGER.error("Exception handling value: "+value);
                        LOGGER.error(e);                        
                        return null;
                    }
                })
                .mapValues((k, v) -> {
                    LOGGER.debug(v);
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
                        LOGGER.error(ex);
                        LOGGER.error("Exception handling value: "+v);
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
            LOGGER.error(throwable);
            LOGGER.error("filterStream Uncaught exception in Thread {0} - {1}", new Object[]{thread, throwable.getMessage()});
            throwable.printStackTrace();
            LOGGER.warn("Stream shutting down following the exception..");
            filterStream.close();
            LOGGER.info("trying to recover stream..");
            filterStream.start();
        });
        filterStream.start();
        LOGGER.info("Stream Initiated");
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
