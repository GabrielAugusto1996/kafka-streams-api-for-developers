package com.learnkafkastreams.serds;

import com.learnkafkastreams.domain.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    private SerdesFactory(){}

    public static Serde<Order> Order() {
        JsonSerializer<Order> orderJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> orderJsonDeserializer = new JsonDeserializer<>(Order.class);

        return Serdes.serdeFrom(orderJsonSerializer, orderJsonDeserializer);
    }
}
