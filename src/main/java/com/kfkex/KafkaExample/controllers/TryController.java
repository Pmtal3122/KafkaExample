package com.kfkex.KafkaExample.controllers;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/try")
@RequiredArgsConstructor
@Slf4j
public class TryController {

    @Value("classpath:data/tryJson.json")
    private Resource tryJson;

    private final KafkaTemplate<@NonNull Integer, @NonNull String> kafkaTemplate;
    private final JsonMapper jsonMapper;

    @PostMapping
    public void sendMessage() throws IOException {

//        log.info("tryJson: {}", tryJson);
//        log.info("tryJson bytes {}", Files.readAllBytes(tryJson.getFilePath()));
//        log.info("tryJson jsonNode {}", jsonMapper.readValue(Files.readAllBytes(tryJson.getFilePath()), JsonNode.class).toPrettyString());
//        log.info("tryJson string {}", jsonMapper.writeValueAsString(Files.readAllBytes(tryJson.getFilePath())));

        JsonNode jsonNode = jsonMapper.readValue(Files.readAllBytes(tryJson.getFilePath()), JsonNode.class);
        String message = jsonNode.toString();

        log.info("[sendMessage] Sending message to kafka");
        CompletableFuture<SendResult<@NonNull Integer, @NonNull String>> sendingTemplateMessage = kafkaTemplate.send("topic1", message);
        sendingTemplateMessage.whenCompleteAsync((result, ex) -> {
            log.info("The result is {}", result.getProducerRecord().value());
        });
    }
}
