package com.kfkex.KafkaExample.controllers;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/try")
@RequiredArgsConstructor
@Slf4j
public class TryController {

    private final KafkaTemplate<@NonNull Integer, @NonNull String> kafkaTemplate;

    @PostMapping
    public void sendMessage() {
        log.info("[sendMessage] Sending message to kafka");
        CompletableFuture<SendResult<@NonNull Integer, @NonNull String>> sendingTemplateMessage = kafkaTemplate.send("topic1", "Sending template message");
        sendingTemplateMessage.whenCompleteAsync((result, ex) -> {
            log.info("The result is {}", result.getProducerRecord().value());
        });
    }
}
