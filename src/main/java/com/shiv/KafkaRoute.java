package com.shiv;

import org.apache.camel.Exchange;
//it is camel component which is use to send data
import org.apache.camel.Processor;
//it is camel component which is use to process custom logic in the message
import org.apache.camel.builder.RouteBuilder;
//it is use to create camel route
import org.eclipse.microprofile.config.inject.ConfigProperty;
//use to make application read values from config file
import jakarta.enterprise.context.ApplicationScoped;
//use to make a single instance which can be used inside the whole code
import java.time.LocalDateTime;
//use for local date and time
import java.time.format.DateTimeFormatter;
//use for date formatting
import java.util.Map;
//use for map , as I am useing processor to custom process the data

@ApplicationScoped
public class KafkaRoute extends RouteBuilder {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrap;
    // it is URL of the kafka borker from application.properties

    @Override
    public void configure() {
//it is the default method in which we writing logic
        // Step 1: Produce a message to topic1 every 10s
        from("timer://jsonProducer?period=10000")
                .routeId("ProducerToTopic1")
                .setBody().simple("{\"name\": \"Shivendra\", \"status\": \"new\", \"timestamp\": \"${date:now:yyyy-MM-dd HH:mm:ss}\"}")
                .log("💥 Producing to topic1: ${body}")
                .to("kafka:topic1?brokers=" + kafkaBootstrap);
        // in from i wrote what to do and send , like
        //a timer for in every 10 produce a json from custom routeID (ProducerToTopic1) helps in tracking
        //setbody is used to create simple json which name, status, and timestamp
        //log for tracking
        //to kafka broker topic1 using brocker URL


        // Step 2: Consume from topic1 → modify → produce to topic2
        from("kafka:topic1?brokers=" + kafkaBootstrap + "&autoOffsetReset=earliest")
                .routeId("TransformAndForwardToTopic2")
                .unmarshal().json() // converts JSON to Map
                .log("🔄 Consumed from topic1: ${body}")
                .process(new Processor() {
                    @SuppressWarnings("unchecked")
                    //for tell quarkus, that we dont want any warning for this, as this is custom processor
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        Map<String, Object> body = exchange.getIn().getBody(Map.class);
                        body.put("name", "Shivendra Shukla");
                        body.put("status", "processed");
                        body.put("processedAt", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                        exchange.getIn().setBody(body);
                    }
                })
                .marshal().json()
                .log("➡️ Producing to topic2: ${body}")
                .to("kafka:topic2?brokers=" + kafkaBootstrap);

        //from brokcer URL, where offset is reset to earliest, as i have only on partition and offset
        //through routeID then unmarshing/changing the json to java map
        //logging to show we are consuming from topic1
        //now processing it by using procesor then suppressing the warning
        //then in process method passing the object of exchnage containing the data in java object format
        //chnageing name shivendra to shivendra shukla
        //changing status new to processed
        //adding processedAT with the timestamp
        //then again setting it in the body of the java object which is passed in the method
        //the marshing/changing java object to json
        //loggin the message producing to Topic2
        //to topic2

        // Step 3: Consume from topic2 and log
        from("kafka:topic2?brokers=" + kafkaBootstrap + "&autoOffsetReset=earliest")
                .routeId("ConsumerTopic2Logger")
                .log("✅ Final message from topic2: ${body}");
        //it simple fetch data from topic2 through routeID and give log


    }
}
