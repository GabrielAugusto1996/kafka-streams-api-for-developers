package com.learnkafkastreams.exceptionhandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;


/**
 * Custom Exception for Topology component, options:
 * <p>
 * REPLACE_THREAD: Replace the thread for another one and retry again
 * SHUTDOWN_CLIENT: Shutdown the specific task
 * SHUTDOWN_APPLICATION: Shutdown all the application
  */
@Slf4j
public class StreamsProcessorCustomExceptionHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
        log.error("Exception in the application: {}", exception.getMessage(), exception);

        if (exception instanceof StreamsException) {
            Throwable cause = exception.getCause();

            if (cause.getMessage().equals("Transient Error")) {
                return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            }
        }

        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
