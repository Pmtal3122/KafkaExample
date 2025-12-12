package com.kfkex.KafkaExample.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TryListener {

    private static final String GROUP_1 = "group1";

    @KafkaListener(topics = "topic1", groupId = "group1", id = "listener1", autoStartup = "true", containerFactory = "kafkaListenerContainerFactory")
    public void listen(String data) {
        log.info("[tryListener] Group: {}, Message: {}", GROUP_1, data);
    }
}


