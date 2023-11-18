package com.learnkafkastreams.serds;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    private SerdesFactory(){}

    public static Serde<Greeting> Greeting() {
        JsonSerializer<Greeting> greetingJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Greeting> greetingJsonDeserializer = new JsonDeserializer<>(Greeting.class);

        return Serdes.serdeFrom(greetingJsonSerializer, greetingJsonDeserializer);
    }
}
