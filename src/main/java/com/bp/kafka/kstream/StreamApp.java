/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bp.kafka.kstream;

;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
/**
 *
 * @author Birender Pal
 */
public class StreamApp {

    private static final String logDir = System.getProperty("app.log.dir");
    private static Logger LOGGER = LogManager.getLogger(StreamApp.class);
    private static String propFile;
    private final static String HOSTNAME = "0.0.0.0";
    static int port = 8000;

    private static void app(String runType) throws IOException, Exception {
        String appServer = HOSTNAME + ":" + Integer.toString(port);
        FilterStream kstream=null;
        switch (runType) {
            case "test":
                System.out.println(runType);
                kstream = new FilterStream();
            case "dev":
                kstream = new FilterStream(propFile,appServer, "filterapp", "localhost:9092", "dev-topic");
            case "prod":
                kstream = new FilterStream(propFile);
        }
        

        KafkaStreams streams = kstream.createFilterStream();
        System.out.println("Stream initiated");

        final RestInterface restService = new RestInterface();
        restService.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("Exiting......");
                try {
                    streams.close();
                    LOGGER.info("Kafka Stream services stopped");

                    restService.stop();
                    LOGGER.info("REST services stopped");

                } catch (Exception ex) {
                    //log & continue....
                    LOGGER.error(ex::getMessage);
                }
            }
        }));
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            //System.out.println("ERROR: No property file specified");
            LOGGER.info("No property file specified, looking for property file in config directory");
            File jarFile = new File(StreamApp.class.getProtectionDomain().getCodeSource().getLocation().getPath());
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
        app("test");
    }
}
