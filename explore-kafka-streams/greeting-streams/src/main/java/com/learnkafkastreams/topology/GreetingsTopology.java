package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.kstream.Printed.toSysOut;

public class GreetingsTopology {

    public static final String GREETINGS = "greetings";
    public static final String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology buildTopology() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));

        greetingsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

        KStream<String, String> modifiedStream = greetingsStream
                //.filter((key, value) -> value.length()<5)
                //.filterNot((key, value) -> value.length()<5)
                //.map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase())) //Convert key and value
                //.mapValues((readOnlyKey, value) -> value.toUpperCase()); //Convert just the value
                        .flatMap((key, value) -> {
                            List<String> newValues = Arrays.asList(value.split(""));


                            return newValues
                                    .stream()
                                    .map(newValue -> KeyValue.pair(key, newValue.toUpperCase()))
                                    .collect(Collectors.toList());
                        });

        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));


        modifiedStream
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}
