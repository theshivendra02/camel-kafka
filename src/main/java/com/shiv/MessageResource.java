package com.shiv;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.camel.ProducerTemplate;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@Path("/message")
public class MessageResource {

    private static final Logger LOG = Logger.getLogger(MessageResource.class);

    @Inject
    ProducerTemplate producerTemplate;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrap;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response sendMessage(String jsonPayload) {
        LOG.infof("📩 Received POST /message with payload: %s", jsonPayload);
        LOG.infof("📤 Sending to Kafka topic2 using broker: %s", kafkaBootstrap);

        producerTemplate.sendBody("kafka:topic2?brokers=" + kafkaBootstrap, jsonPayload);

        LOG.info("✅ Message successfully sent to topic2");
        return Response.ok("Message sent to topic2").build();
    }
}
