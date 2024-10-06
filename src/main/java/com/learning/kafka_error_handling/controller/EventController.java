package com.learning.kafka_error_handling.controller;

import com.learning.kafka_error_handling.dto.User;
import com.learning.kafka_error_handling.publisher.KafkaMessagePublisher;
import com.learning.kafka_error_handling.utils.CsvReaderUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/producer-app")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;

    @PostMapping("/publishNew")
    public ResponseEntity<?> publishEvent(@RequestBody User user) {
        try {
            publisher.sendEvents(user);
            return ResponseEntity.ok("Message published successfully");
        } catch (Exception exception) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/validate-csv")
    public ResponseEntity<?> publishEventToValidateCSV() {
        try {
            Optional.ofNullable(CsvReaderUtil.readDataFromCsv()).ifPresent((users) -> {
                for (User usr : users) {
                    publisher.sendEvents(usr);
                }
            });
            return ResponseEntity.ok("Acknowledged");
        } catch (Exception exception) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
