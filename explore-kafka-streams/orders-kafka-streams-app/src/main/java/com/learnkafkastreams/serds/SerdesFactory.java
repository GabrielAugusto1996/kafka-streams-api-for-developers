package com.learnkafkastreams.serds;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Revenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    private SerdesFactory(){}

    public static Serde<Order> Order() {
        JsonSerializer<Order> orderJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> orderJsonDeserializer = new JsonDeserializer<>(Order.class);

        return Serdes.serdeFrom(orderJsonSerializer, orderJsonDeserializer);
    }

    public static Serde<Revenue> Revenue() {
        JsonSerializer<Revenue> revenueJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Revenue> revenueJsonDeserializer = new JsonDeserializer<>(Revenue.class);

        return Serdes.serdeFrom(revenueJsonSerializer, revenueJsonDeserializer);
    }
}
