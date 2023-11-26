package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.state.KeyValueStore;

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
        exploreAggregate(groupStream);

        return streamsBuilder.build();
    }

    private static void exploreCount(KGroupedStream<String, String> groupStream) {
        KTable<String, Long> countByAlphabet = groupStream
                .count(Named.as(COUNT_PER_ALPHABETIC), Materialized.as(COUNT_PER_ALPHABETIC));

        countByAlphabet
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(COUNT_PER_ALPHABETIC));
    }

    private static void exploreReduce(KGroupedStream<String, String> groupStream) {
        KTable<String, String> reduce = groupStream
                .reduce((value1, value2) -> {
                    log.info("Value 1: {}, Value 2: {}", value1, value2);

                    return value1.toUpperCase() + value2.toUpperCase();
                }, Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce-words")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String()));

        reduce
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("reduce-words"));
    }

    private static void exploreAggregate(KGroupedStream<String, String> groupStream) {
        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer = AlphabetWordAggregate::new;

        Aggregator<String, String, AlphabetWordAggregate> aggregator
                = (key, value, aggregate) -> aggregate.updateNewEvents(key, value);


        var aggregatedStream = groupStream
                .aggregate(alphabetWordAggregateInitializer,
                        aggregator,
                        Materialized.<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>
                                        as("aggregated-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.alphabetWordAggregate())
                );

        aggregatedStream
                .toStream()
                .print(Printed.<String,AlphabetWordAggregate>toSysOut().withLabel("aggregated-words"));
    }

}
