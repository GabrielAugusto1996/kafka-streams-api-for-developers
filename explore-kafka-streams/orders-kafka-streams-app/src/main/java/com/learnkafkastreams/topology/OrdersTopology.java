package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.serds.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.List;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String STORES = "stores";

    public static Topology buildTopology() {
        var streamBuilder = new StreamsBuilder();

        KStream<String, Order> orderStream = streamBuilder
                .stream(List.of(ORDERS, STORES), Consumed.with(Serdes.String(), SerdesFactory.Order()));

        orderStream.print(Printed.<String, Order>toSysOut().withLabel("orders"));

        ValueMapper<Order, Revenue> revenueMapper = order -> new Revenue(order.locationId(), order.finalAmount());


        // This code can be used to split kafka topics and send it from different topics or do different logic
        Predicate<String, Order> generalPredicate = (key, value) -> value.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, value) -> value.orderType().equals(OrderType.RESTAURANT);

        orderStream
                .split(Named.as("General-Restaurant-Stream"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {

                    generalOrderStream
                            .print(Printed.<String, Order>toSysOut().withLabel("general"));

                    generalOrderStream
                            .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
                            .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.Revenue()));
                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {

                    restaurantOrderStream
                            .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
                            .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.Revenue()));

                    restaurantOrderStream
                            .print(Printed.<String, Order>toSysOut().withLabel("restaurant"));
                }));

        return streamBuilder.build();
    }
}
