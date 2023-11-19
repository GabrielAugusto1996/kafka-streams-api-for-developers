package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

// Custom Exception for Deserialization component
@Slf4j
public class StreamsSerializationExceptionHandler implements ProductionExceptionHandler {
    private int errorCount = 0;

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Exception is: {}, and the Kafka Recorder is: {}", exception.getMessage(), record, exception);

        log.error("Error Counter is: {}", errorCount);
        if (errorCount < 2) {
            errorCount++;

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        return ProductionExceptionHandlerResponse.FAIL; //Shutdown the application
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
