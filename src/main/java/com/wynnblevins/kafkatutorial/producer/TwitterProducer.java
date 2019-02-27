package com.wynnblevins.kafkatutorial.producer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.wynnblevins.kafkatutorial.config.TwitterConfig;
import com.wynnblevins.kafkatutorial.factory.ProducerFactory;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    
    ProducerFactory<String, String> producerFactory = new ProducerFactory<String, String>();
    
    // I've picked some topics that are going to be tweeted about a bunch
    List<String> terms = Lists.newArrayList("Donald Trump", "CNN", "USA", "Nancy Pelosi", 
    		"Hillary Clinton", "Paul Ryan", "Fox News");

    public TwitterProducer() {}

    public void run(){

        logger.info("Setup...");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create a twitter client
        Client client = createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        
        // create a kafka producer
        KafkaProducer<String, String> producer = producerFactory.createKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        // loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                logger.error("Ooopsie woopsie... error encountered...");
                client.stop();
            }
            if (msg != null){
                logger.info(msg);
                
                // tweets are going to a kafka topic called twitter tweets
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Encountered error", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }
    
    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);
        TwitterConfig config = new TwitterConfig();
        Properties props = config.getTwitterConfig();
        
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
        		(String) props.get("consumerKey"), 
        		(String) props.get("consumerSecret"), 
        		(String) props.get("token"), 
        		(String) props.getProperty("secret"));

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    
}