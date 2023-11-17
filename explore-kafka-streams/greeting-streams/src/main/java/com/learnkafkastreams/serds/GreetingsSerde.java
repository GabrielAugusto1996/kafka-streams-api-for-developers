package com.learnkafkastreams.serds;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class GreetingsSerde implements Serde<Greeting> {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false); // We need both of these configurations to work with Java Date API (LocalDate, LocalDateTime and etc)

    @Override
    public Serializer<Greeting> serializer() {
        return new GreetingSerializer(this.objectMapper);
    }

    @Override
    public Deserializer<Greeting> deserializer() {
        return new GreetingsDeserializer(this.objectMapper);
    }
}
