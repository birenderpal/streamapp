/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bp.kafka.kstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

/**
 *
 * @author t821012
 */
@Path("/")
public final class RestInterface {
    private final String incomingStoreName;
    private final String outgoingStoreName;
    private HttpServer server;
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private final static String HOSTNAME = "0.0.0.0";
    static int port = 8000;
    
    private static final Logger LOGGER = LogManager.getLogger(RestInterface.class);
    
    public void start() throws Exception {
        URI baseUri = UriBuilder.fromUri("http://" + HOSTNAME + "/").port(port).build();
        ResourceConfig rc = new ResourceConfig(RestInterface.class);
        server = GrizzlyHttpServerFactory.createHttpServer(baseUri, rc);
        //server.start();    
    }
    
    public void stop() throws Exception {
        if (server != null) {
            server.stop();
        }
    }    
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
    public JsonNode all_outgoing() throws Exception {
        System.out.println("GET request for outgoing");
        LOGGER.debug("GET request for outgoing");
        JsonNode json=null;
        //MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
        try {
            ArrayList<String> outgoing = new ArrayList<>() ;
            //HostInfo hostInfo = new HostInfo("localhost",port);
            //System.out.println(storeName);
            KafkaStreams ks = FilterStream.filterStream;           
            ReadOnlyKeyValueStore<String, String> outgoingStore = FilterStream.waitUntilStoreIsQueryable(outgoingStoreName, QueryableStoreTypes.keyValueStore(), ks);
                    //ks.store(storeName, QueryableStoreTypes.<String,String>keyValueStore());
            KeyValueIterator<String, String> storeIterator = outgoingStore.all();
            //System.out.println(indStore.approximateNumEntries());
            while (storeIterator.hasNext()) {
                KeyValue<String, String> kv = storeIterator.next();
                //JsonNode json = MAPPER.readTree(kv.value); 
                outgoing.add(kv.value);
            }
            String message = "{\"outgoing\":"+outgoing.toString()+"}";
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
    public JsonNode all_incoming() throws Exception {
        LOGGER.debug("GET for incoming");
        JsonNode json=null;
        try {
            ArrayList<String> incoming = new ArrayList<>() ;
            //HostInfo hostInfo = new HostInfo("localhost",port);
            //System.out.println(storeName);
            KafkaStreams ks = FilterStream.filterStream;
            ReadOnlyKeyValueStore<String, String> incomingStore = FilterStream.waitUntilStoreIsQueryable(incomingStoreName, QueryableStoreTypes.keyValueStore(), ks);
                    //ks.store(storeName, QueryableStoreTypes.<String,String>keyValueStore());
            KeyValueIterator<String, String> storeIterator = incomingStore.all();
            
            while (storeIterator.hasNext()) {
                KeyValue<String, String> kv = storeIterator.next();
                //JsonNode json = MAPPER.readTree(kv.value); 
                incoming.add(kv.value);
            }
            String message = "{\"incoming\":"+incoming.toString()+"}";
            //System.out.println(indicators);
            json = MAPPER.readTree(message);                                                            
        } catch (IOException | InterruptedException e){
            LOGGER.error(e);
        }
        return json;
        
    }   
}

