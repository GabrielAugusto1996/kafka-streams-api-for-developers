package com.learnkafkastreams.launcher;

import com.learnkafkastreams.exceptionhandler.StreamsDeserializationExceptionHandler;
import com.learnkafkastreams.exceptionhandler.StreamsProcessorCustomExceptionHandler;
import com.learnkafkastreams.exceptionhandler.StreamsSerializationExceptionHandler;
import com.learnkafkastreams.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
@Slf4j
public class GreetingsStreamApp {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsDeserializationExceptionHandler.class); //It will not stop the application if it has an exception in Deserialization, Default Class: LogAndFail (Stop Application)
        properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsSerializationExceptionHandler.class); //It will not stop the application if it has an exception in Serialization, Default Class: LogAndFail (Stop Application)

        var greetingsTopology = GreetingsTopology.buildTopology();

        var kafkaStream = new KafkaStreams(greetingsTopology, properties);

        kafkaStream.setUncaughtExceptionHandler(new StreamsProcessorCustomExceptionHandler()); // It will change the Application behavior for the Topology

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStream::close));

        try  {
            kafkaStream.start();
        } catch (Exception e) {
            log.error("Failed to start the stream.", e);
        }
    }

    private static void createTopics(Properties config, List<String> topics) {

        AdminClient admin = AdminClient.create(config);

        var partitions = 1;
        short replication  = 1;

        var newTopics = topics
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
           createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }
}
