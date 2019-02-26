package com.wynnblevins.kafkatutorial.config;

import java.io.IOException;
import java.util.Properties;

public class TwitterConfig {
	
	public Properties getTwitterConfig() {
		Properties props = new Properties();
		
		try {
			props.load(TwitterConfig.class.getResourceAsStream("/application.properties"));
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		
		return props;
	}
}
