package com.kfkex.KafkaExample.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class TryListener {

    private static final String GROUP_1 = "group1";
    private final JsonMapper jsonMapper;

    @KafkaListener(topics = "topic1", groupId = "group1", id = "listener1", autoStartup = "true", containerFactory = "kafkaListenerContainerFactory")
    public void listen(List<String> topics) {
        log.info("[tryListener] Group: {}, Message: {}", GROUP_1, topics);
    }
}


