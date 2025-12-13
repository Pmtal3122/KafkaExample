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
    public void listen(String data) {
        log.info("[tryListener] Group: {}, Message: {}", GROUP_1, data);
        JsonNode jsonNode = jsonMapper.convertValue(data, JsonNode.class); // Wrong one

        JsonNode jsonNode1 = jsonMapper.readTree(data); // Correct one

//        log.info("[tryListener] jsonNode1: {}", jsonNode1.toPrettyString());

        List<String> valuesList = jsonNode1.get("data")
                .get("messages")
                .valueStream()
                .map(node -> node.get("value").toString())
                .toList();

        log.info("[tryListener] Values List: {}", valuesList);
    }
}


