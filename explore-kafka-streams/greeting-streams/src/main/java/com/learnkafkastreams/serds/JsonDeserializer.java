package com.learnkafkastreams.serds;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false); // We need both of these configurations to work with Java Date API (LocalDate, LocalDateTime and etc)

    private final Class<T> destinationClass;

    public JsonDeserializer(Class<T> destinationClass) {
        this.destinationClass = destinationClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            log.error("Failed to deserialize, data is null for the class: {}", destinationClass);
            throw new RuntimeException("Failed to deserialize");
        }

        try {
            return this.objectMapper.readValue(data, destinationClass);
        } catch (IOException e) {
            log.error("JsonProcessingError: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }  catch (Exception e) {
            log.error("Exception: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
