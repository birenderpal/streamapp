/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spark.kafka.kstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.ArrayList;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author t821012
 */
@Path("/")
public final class RestInterface {
    private final String incomingStoreName;
    private final String outgoingStoreName;
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    
    private static final Logger LOGGER = LogManager.getLogger(RestInterface.class);
    public RestInterface(){
        incomingStoreName=FilterStream.INCOMING_STORE_NAME;
        outgoingStoreName=FilterStream.OUTGOING_STORE_NAME;
    }
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String root_path() throws Exception {
        return "Home Page";
    }
    
    @Path("/outgoing")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode all_indicators() throws Exception {
        System.out.println("GET request for outgoing");
        LOGGER.debug("GET request for outgoing");
        JsonNode json=null;
        //MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            ArrayList<String> indicators = new ArrayList<>() ;
            //HostInfo hostInfo = new HostInfo("localhost",port);
            //System.out.println(storeName);
            KafkaStreams ks = FilterStream.filterStream;           
            ReadOnlyKeyValueStore<String, String> indStore = FilterStream.waitUntilStoreIsQueryable(outgoingStoreName, QueryableStoreTypes.keyValueStore(), ks);
                    //ks.store(storeName, QueryableStoreTypes.<String,String>keyValueStore());
            KeyValueIterator<String, String> storeIterator = indStore.all();
            //System.out.println(indStore.approximateNumEntries());
            while (storeIterator.hasNext()) {
                KeyValue<String, String> kv = storeIterator.next();
                //JsonNode json = MAPPER.readTree(kv.value); 
                indicators.add(kv.value);
            }
            String message = "{\"outgoing\":"+indicators.toString()+"}";
            //System.out.println(indicators);
            json = MAPPER.readTree(message);
                    
        } catch (IOException | InterruptedException e){
            LOGGER.error(e);
        }
        return json;
        
    }

    @Path("/incoming")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode all_inventory() throws Exception {
        LOGGER.debug("GET for incoming");
        JsonNode json=null;
        try {
            ArrayList<String> inventory = new ArrayList<>() ;
            //HostInfo hostInfo = new HostInfo("localhost",port);
            //System.out.println(storeName);
            KafkaStreams ks = FilterStream.filterStream;
            ReadOnlyKeyValueStore<String, String> invStore = FilterStream.waitUntilStoreIsQueryable(incomingStoreName, QueryableStoreTypes.keyValueStore(), ks);
                    //ks.store(storeName, QueryableStoreTypes.<String,String>keyValueStore());
            KeyValueIterator<String, String> storeIterator = invStore.all();
            
            while (storeIterator.hasNext()) {
                KeyValue<String, String> kv = storeIterator.next();
                //JsonNode json = MAPPER.readTree(kv.value); 
                inventory.add(kv.value);
            }
            String message = "{\"incoming\":"+inventory.toString()+"}";
            //System.out.println(indicators);
            json = MAPPER.readTree(message);                                                            
        } catch (IOException | InterruptedException e){
            LOGGER.error(e);
        }
        return json;
        
    }   
}
