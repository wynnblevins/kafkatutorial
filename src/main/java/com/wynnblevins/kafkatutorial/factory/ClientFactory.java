package com.wynnblevins.kafkatutorial.factory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

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

public class ClientFactory {
	public Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms){
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
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
