/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spark.kafka.kstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class RestInterface {
    private final String invStoreName;
    private final String indStoreName;
    private static final ObjectMapper MAPPER = new ObjectMapper(); 
    private static Logger LOGGER = LogManager.getLogger(RestInterface.class);
    public RestInterface(){
        invStoreName=SplunkStream.INV_STORE_NAME;
        indStoreName=SplunkStream.IND_STORE_NAME;
    }
    @Path("/indicators")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode all_indicators() throws Exception {
        JsonNode json=null;
        try {
            Set<String> indicators = new HashSet<String>() ;
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
            String message = "{\"indicators\":"+indicators.toString()+"}";
            //System.out.println(indicators);
            json = MAPPER.readTree(message);
                    
        } catch (Exception e){
            System.out.println(e);
        }
        return json;
        
    }

    @Path("/inventory")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonNode all_inventory() throws Exception {
        JsonNode json=null;
        try {
            Set<String> inventory = new HashSet<String>() ;
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
        } catch (Exception e){
            System.out.println(e);
        }
        return json;
        
    }
    
    
}
