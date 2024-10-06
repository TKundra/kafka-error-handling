package com.learning.kafka_error_handling.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learning.kafka_error_handling.dto.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Stream;

@Service
@Slf4j
public class KafkaMessageConsumer {
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 3000, multiplier = 1.5, maxDelay = 15000)
    ) // 3 topic N-1
    @KafkaListener(topics = "${app.topic.name}", groupId = "lk-group")
    public void consumeEvents(
            User user,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        try {
            log.info("Received: {} from {} offset {}", new ObjectMapper().writeValueAsString(user), topic, offset);

            // validate restricted IP before process the records
            List<String> restrictedIpList = Stream.of(
                    "32.241.244.236", "15.55.49.164", "81.1.95.253", "126.130.43.183"
            ).toList();

            if (restrictedIpList.contains(user.getIpAddress())) {
                throw new RuntimeException("Invalid IP Address received !");
            }

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    // Dead Letter Topic Handler
    @DltHandler
    public void listenDLT(
            User user,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info(
                "DLT Received: {}, from {}, offset {}",
                user.getFirstName(),
                topic,
                offset
        );
    }
}

/*
 * Exception Cases - don't retry if following exception occurs:
 *
   @RetryableTopic(
        attempts = "4",
        exclude = {NullPointerException.class, RuntimeException.class}
   )
 * */

/* Summary
 * There will be an initial wait of 3 seconds before the first retry, and subsequent retries will have
 * increasing delays based on the multiplier (1.5), but they will not exceed a maximum of 15 seconds.
 *
 * If all attempts fail, the message can be sent to a dead-letter topic (if configured) or logged accordingly.
 *
 * Time Intervals for Each Attempt
 * First Attempt:
 *      Delay: 0 milliseconds (immediate attempt)
 *      Total Time Elapsed: 0 ms
 *
 * Second Attempt:
 *      Delay: 3000 milliseconds (3 seconds)
 *      Total Time Elapsed: 0 ms + 3000 ms = 3000 ms (3 seconds)
 *
 * Third Attempt:
 *      Delay: 3000 ms * 1.5 = 4500 milliseconds (4.5 seconds)
 *      Total Time Elapsed: 3000 ms + 4500 ms = 7500 ms (7.5 seconds)
 *
 * Fourth Attempt:
 *      Delay: 4500 ms * 1.5 = 6750 milliseconds (6.75 seconds)
 *
 * If all attempts fail, the total time elapsed before the last failure would be approximately 14.25 seconds.
 * */