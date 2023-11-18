package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.serds.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

import java.util.List;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static Topology buildTopology() {
        var streamBuilder = new StreamsBuilder();

        streamBuilder
                .stream(List.of(ORDERS, STORES), Consumed.with(Serdes.String(), SerdesFactory.Order()))
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));

        return streamBuilder.build();
    }
}
