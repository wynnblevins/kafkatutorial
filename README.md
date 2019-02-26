# Kafka Tutorial Project
This project takes tweets concerning a specific subject, in this case ubiquitous political terms/names, and prints the tweets to the console.

## Running locally
Its assumed that you have a twitter developer account with the necessary key values created.  Within src/main/resources/application.properties, set these values appropriately.

Also assumed is that you have kafka server running locally.  If your kafka installation is in your home folder, the command to start the kafka server is:
```
bin/kafka-server-start.sh config/server.properties
```