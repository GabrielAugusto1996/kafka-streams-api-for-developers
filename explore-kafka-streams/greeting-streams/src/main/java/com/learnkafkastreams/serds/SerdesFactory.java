package com.learnkafkastreams.serds;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;

public class SerdesFactory {

    private SerdesFactory(){}

    public static Serde<Greeting> Greeting() {
        return new GreetingsSerde();
    }
}
