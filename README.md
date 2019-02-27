# Kafka Tutorial Project
This project takes tweets concerning a specific subject, in this case ubiquitous political terms/names, and prints the tweets to the console.

## Running Locally
### Prerequisites
You will need to have a twitter developer account with the necessary key values created.  Within src/main/resources/application.properties, set these values appropriately.

### Starting Local Zookeeper & Kafka
Start zookeeper locally.  If your kafka installation is in your home folder, the command to start the zookeeper server is:
```
~/kafka-install-dir/bin/zookeeper-server-start.sh ~/kafka-install-dir/config/zookeeper.properties
```
Also needed is a kafka server running locally.  If your kafka installation is in your home folder, the command to start the kafka server is:
```
~/kafka-install-dir/bin/kafkafka-server-start.sh ~/kafka-install-dir/config/server.properties
```

### Building and Running
To package and run the application using maven run the following command:
```
mvn package exec:java
``` 
You can also optionally clean the project before building:
```
mvn clean package exec:java
``` 