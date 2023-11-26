package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static final String COUNT_PER_ALPHABETIC = "count-per-alphabetic";
    public static String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder
                .stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));

        inputStream
                .print(Printed.<String, String>toSysOut().withLabel(AGGREGATE));

        var groupStream = inputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
                        // .groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String())); We can use this method to change the key value

        exploreCount(groupStream);
        exploreReduce(groupStream);

        return streamsBuilder.build();
    }

    private static void exploreReduce(KGroupedStream<String, String> groupStream) {
        KTable<String, String> reduce = groupStream
                .reduce((value1, value2) -> {
                    log.info("Value 1: {}, Value 2: {}", value1, value2);

                    return value1.toUpperCase() + value2.toUpperCase();
                });

        reduce
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("reduce-words"));
    }

    private static void exploreCount(KGroupedStream<String, String> groupStream) {
        KTable<String, Long> countByAlphabet = groupStream
                .count(Named.as(COUNT_PER_ALPHABETIC));

        countByAlphabet
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(COUNT_PER_ALPHABETIC));
    }

}
