package com.shiv;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@ApplicationScoped
public class KafkaRoute extends RouteBuilder {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrap;

    @Override
    public void configure() {

        // Step 1: Produce a message to topic1 every 10s
        from("timer://jsonProducer?period=10000")
                .routeId("ProducerToTopic1")
                .setBody().simple("{\"name\": \"Shivendra\", \"status\": \"new\", \"timestamp\": \"${date:now:yyyy-MM-dd HH:mm:ss}\"}")
                .log("💥 Producing to topic1: ${body}")
                .to("kafka:topic1?brokers=" + kafkaBootstrap);

        // Step 2: Consume from topic1 → modify → produce to topic2
        from("kafka:topic1?brokers=" + kafkaBootstrap + "&autoOffsetReset=earliest")
                .routeId("TransformAndForwardToTopic2")
                .unmarshal().json() // converts JSON to Map
                .log("🔄 Consumed from topic1: ${body}")
                .process(new Processor() {
                    @SuppressWarnings("unchecked")
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

        // Step 3: Consume from topic2 and log
        from("kafka:topic2?brokers=" + kafkaBootstrap + "&autoOffsetReset=earliest")
                .routeId("ConsumerTopic2Logger")
                .log("✅ Final message from topic2: ${body}");
    }
}
