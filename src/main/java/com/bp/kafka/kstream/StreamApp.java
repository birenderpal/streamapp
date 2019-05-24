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
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
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
    private static Properties props = new Properties();
    
    /*
    
        Method to run the app without the property file and read configuration from Utils class.
    
    */
    private static void app(String runType) throws IOException, Exception {         
        Utils utils = new Utils();
        switch (runType) {
            case "test":
                props = utils.getTestProperties();
                FilterStream kstream = new FilterStream(props);
                init(kstream);
                break;
            case "dev":
                props = utils.getDevProperties();
                kstream = new FilterStream(props);
                init(kstream);
                break;
        }

    }

   private static void app() throws IOException, Exception {
        Utils utils = new Utils(propFile);        
        props = utils.getProperties();
        FilterStream kstream=new FilterStream(props);        
        init(kstream);
    }     
   
   private static void init(FilterStream kstream) throws Exception{
        KafkaStreams streams = kstream.createFilterStream();
        String[] restEndpoint = props.getProperty(StreamsConfig.APPLICATION_SERVER_CONFIG).split(":");        
        final RestInterface restService = new RestInterface();
        restService.setHostname(restEndpoint[0].toString());
        restService.setPort(Integer.parseInt(restEndpoint[1].toString()));
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

        /*
         In production to be run with app(propFile)
        */        
        app();
        
        //LOGGER.info("app started with test configuraiton");
       // app("test");
    }
}
