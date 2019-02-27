package com.wynnblevins.kafkatutorial.producer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import com.wynnblevins.kafkatutorial.factory.ClientFactory;
import com.wynnblevins.kafkatutorial.factory.ProducerFactory;

import org.apache.kafka.clients.producer.*;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    
    ProducerFactory<String, String> producerFactory = new ProducerFactory<String, String>();
    ClientFactory clientFactory = new ClientFactory();
    
    // I've picked some topics that are going to be tweeted about a bunch
    List<String> terms = Lists.newArrayList("Donald Trump", "CNN", "USA", "Nancy Pelosi", 
    		"Hillary Clinton", "Paul Ryan", "Fox News");

    public TwitterProducer() {}

    public void run() {
        logger.info("Setup...");

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create a twitter client and attempt to establish a connection.
        Client client = clientFactory.createTwitterClient(msgQueue, terms);
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
}