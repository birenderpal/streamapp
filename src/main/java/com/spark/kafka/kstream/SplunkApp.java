/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spark.kafka.kstream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Properties;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

/**
 *
 * @author t821012
 */
public class SplunkApp {

    private static final String logDir = System.getProperty("app.log.dir");
    private static Logger LOGGER = LogManager.getLogger(SplunkApp.class);
    private static String propFile;
    private final static String HOSTNAME = "0.0.0.0";
    static int port = 8000;

    private static void app() throws IOException, Exception {
        String appServer = HOSTNAME + ":" + Integer.toString(port);
        SplunkStream kstream = new SplunkStream(propFile, appServer);

        KafkaStreams streams = kstream.createSplunkStream();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("shutting down stream app...");
                streams.close();
            }
        }));
        ServletContextHandler context
                = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        Server jettyServer = new Server(port);
        ResourceConfig rc = new ResourceConfig();
        rc.register(RestInterface.class);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        jettyServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("Exiting......");
                try {
                    streams.close();
                    LOGGER.info("Kafka Stream services stopped");

                    jettyServer.stop();
                    LOGGER.info("REST services stopped");

                } catch (Exception ex) {
                    //log & continue....
                    LOGGER.error(ex::getMessage);
                }
            }
        }));
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            //System.out.println("ERROR: No property file specified");
            LOGGER.info("No property file specified, looking for property file in config directory");
            File jarFile = new File(SplunkApp.class.getProtectionDomain().getCodeSource().getLocation().getPath());
            String path = jarFile.getParentFile().getPath();
            try {
                String decodedPath = URLDecoder.decode(path, "UTF-8");
                propFile = decodedPath + "/conf/" + "stream.properties";
            } catch (UnsupportedEncodingException ex) {
                LOGGER.error(ex);
            } catch (Exception ex) {
                LOGGER.error(ex);
            }

        } else {
            propFile = args[0];
        }
    }
}
