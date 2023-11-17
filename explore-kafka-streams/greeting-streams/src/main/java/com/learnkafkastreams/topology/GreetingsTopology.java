package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingsTopology {

    public static final String GREETINGS = "greetings";
    public static final String GREETINGS_UPPERCASE = "greetings_uppercase";
    public static final String GREETINGS_SPANISH = "greetings_spanish";

    public static Topology buildTopology() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> greetingsStream = streamsBuilder
                .stream(GREETINGS
                        // Consumed.with(Serdes.String(), Serdes.String())
                );

        KStream<String, String> greetingsSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH
                        // Consumed.with(Serdes.String(), Serdes.String())
                );

        KStream<String, String> mergeStream = greetingsStream
                .merge(greetingsSpanishStream);

        mergeStream.print(Printed.<String, String>toSysOut().withLabel("mergeStream"));

        KStream<String, String> modifiedStream = mergeStream
                .filter((key, value) -> value.length()>1)
                .peek((key, value) -> log.info("After Filter, Key; {}, value: {}", key, value)) //It´s used for logging or debug operators
                //.filterNot((key, value) -> value.length()<5)
                .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))
                .peek((key, value) -> log.info("After Map, Key; {}, value: {}", key, value));
                //.mapValues((readOnlyKey, value) -> value.toUpperCase()); //Convert just the value
                        /*.flatMap((key, value) -> {
                            List<String> newValues = Arrays.asList(value.split(""));


                            return newValues
                                    .stream()
                                    .map(newValue -> KeyValue.pair(key, newValue.toUpperCase()))
                                    .collect(Collectors.toList());
                        });*/

        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));


        modifiedStream
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}
