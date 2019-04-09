/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.spark.kafka.kstream;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 *
 * @author t821012
 */
public class ConfigureStream {

    private static String propFile;
    public StreamsBuilder builder;
    private static Properties props = null;
    private static KafkaStreams streams;
    private static Topology topology;
    final static Logger LOGGER = LogManager.getLogger(ConfigureStream.class);
      ConfigureStream (String fileName) {
          LOGGER.debug("Using property file {} for stream configuration ",fileName);
        this.propFile = fileName;
        ConfigureStream.props = new Properties();
        //InputStream input = null;
        try {
            InputStream input = new FileInputStream(this.propFile);
            ConfigureStream.props.load(input);
            ConfigureStream.props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            ConfigureStream.props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        } catch (FileNotFoundException e1) {
            // Log for property file not found.
            LOGGER.error(e1);
            //e1.printStackTrace();
        }
        catch (IOException e){
            // Log for property file not loaded.
            LOGGER.error(e);
        }  
        builder = new StreamsBuilder();
        //return builder;
    }
      public void initiate(){
        ConfigureStream.topology = builder.build();
        ConfigureStream.streams = new KafkaStreams(topology, props);        

      }
      public void startStream(){
            ConfigureStream.streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            // here you should examine the throwable/exception and perform an appropriate action!
            LOGGER.error(thread, throwable);
        });

        ConfigureStream.streams.start();
      }
      
      public void cleanUpStream(){
          ConfigureStream.streams.cleanUp();
      }
      public void stopStream(){
          ConfigureStream.streams.close();
      }

}
