/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spark.kafka.kstream;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
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
    private final String invStoreName;
    private final String indStoreName;
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    
    private static final Logger LOGGER = LogManager.getLogger(RestInterface.class);
    public RestInterface(){
        invStoreName=SplunkStream.INV_STORE_NAME;
        indStoreName=SplunkStream.IND_STORE_NAME;
    }
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String root_path() throws Exception {
        return "Home Page";
    }
    
    @Path("/messages")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode all_indicators() throws Exception {
        System.out.println("GET request for indicators");
        LOGGER.debug("GET request for indicators");
        JsonNode json=null;
        //MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            ArrayList<String> indicators = new ArrayList<>() ;
            //HostInfo hostInfo = new HostInfo("localhost",port);
            //System.out.println(storeName);
            KafkaStreams ks = SplunkStream.splunkStream;            
            ReadOnlyKeyValueStore<String, String> indStore = SplunkStream.waitUntilStoreIsQueryable(indStoreName, QueryableStoreTypes.keyValueStore(), ks);
                    //ks.store(storeName, QueryableStoreTypes.<String,String>keyValueStore());
            KeyValueIterator<String, String> storeIterator = indStore.all();
            //System.out.println(indStore.approximateNumEntries());
            while (storeIterator.hasNext()) {
                KeyValue<String, String> kv = storeIterator.next();
                //JsonNode json = MAPPER.readTree(kv.value); 
                indicators.add(kv.value);
            }
            String message = "{\"messages\":"+indicators.toString()+"}";
            //System.out.println(indicators);
            json = MAPPER.readTree(message);
                    
        } catch (IOException | InterruptedException e){
            LOGGER.error(e);
        }
        return json;
        
    }

    @Path("/inventory")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode all_inventory() throws Exception {
        LOGGER.debug("GET for inventory");
        JsonNode json=null;
        try {
            ArrayList<String> inventory = new ArrayList<>() ;
            //HostInfo hostInfo = new HostInfo("localhost",port);
            //System.out.println(storeName);
            KafkaStreams ks = SplunkStream.splunkStream;
            ReadOnlyKeyValueStore<String, String> invStore = SplunkStream.waitUntilStoreIsQueryable(invStoreName, QueryableStoreTypes.keyValueStore(), ks);
                    //ks.store(storeName, QueryableStoreTypes.<String,String>keyValueStore());
            KeyValueIterator<String, String> storeIterator = invStore.all();
            //System.out.println(indStore.approximateNumEntries());
            while (storeIterator.hasNext()) {
                KeyValue<String, String> kv = storeIterator.next();
                //JsonNode json = MAPPER.readTree(kv.value); 
                inventory.add(kv.value);
            }
            String message = "{\"inventory\":"+inventory.toString()+"}";
            //System.out.println(indicators);
            json = MAPPER.readTree(message);                                                            
        } catch (IOException | InterruptedException e){
            LOGGER.error(e);
        }
        return json;
        
    }   
}
