# Update Kafka Producer For Next Project
Dear developers, our next project currently have to deploy some additional function that need producer to be updated!

NOTE :
- Make sure that you **run a test before tommorow** to confirm that it fit with your current configuration.
## Download Cmdline
```
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/jks-lab/Update-Producer-for-Next-Project/main/KafkaProducer.java" -OutFile "KafkaProducer.java"
```
## File Structure
```
SpringKafkaDemo/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── example/
│   │   │           └── SpringKafkaDemo/
│   │   │               ├── controller/
│   │   │               │   └── MessageController.java        // Your existing REST controller
│   │   │               ├── model/
│   │   │               │   └── KafkaMessage.java             // Kafka message model class
│   │   │               ├── consumer/
│   │   │               │   ├── KafkaConsumer.java            // Current Kafka Consumer
│   │   │               ├── producer/
│   │   │               │   ├── KafkaProducer.java            // Replace new Producer here!!
│   │   │               └── SpringKafkaDemoApplication.java   // Main Spring Boot application
│   │   └── resources/
│   │       └── application.yaml                              // YAML configuration file
├── pom.xml                                                   // Maven configuration file
└── README.md   
```
## Kafka Producer Web Interface
```
http://localhost:8899
```
## Kafka topic
```
example-topic
```
## Kafka Consumer
```
Just run the application and it will show up in console.
```
## Kafka topic
```
example-topic
```
## Requirements to run the template
```
Zookeeper
Kafka
```
