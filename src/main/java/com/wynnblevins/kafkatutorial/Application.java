package com.wynnblevins.kafkatutorial;

import com.wynnblevins.kafkatutorial.producer.TwitterProducer;

public class Application {
    public static void main(String[] args) {
        new TwitterProducer().run();
    }
}