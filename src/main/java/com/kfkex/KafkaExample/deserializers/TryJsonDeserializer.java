package com.kfkex.KafkaExample.deserializers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.List;
import java.util.Objects;

@Slf4j
public class TryJsonDeserializer implements Deserializer<List<String>> {

    private static final String INVALID_ARGUMENT_MESSAGE = "Couldn't find json node with the string value ";

    private final JsonMapper jsonMapper;

    public TryJsonDeserializer() {
        this.jsonMapper = new JsonMapper();
    }

    @Override
    public List<String> deserialize(String topic, byte[] bytes) {
        try {
            if (bytes == null || bytes.length == 0) {
                log.error("[TryJsonDeserializer] The bytes array is empty");
                return List.of();
            }
            log.info("[TryJsonDeserializer] string: {}", topic);

            JsonNode jsonNode = jsonMapper.readTree(bytes);
            JsonNode dataNode = jsonNode.get("data");
            if (dataNode == null) {
                log.error("[TryJsonDeserializer] " + INVALID_ARGUMENT_MESSAGE + "data");
                return List.of();
            }

            JsonNode messagesNode = dataNode.get("messages");
            if (messagesNode == null) {
                log.error("[TryJsonDeserializer] " + INVALID_ARGUMENT_MESSAGE + "messages");
                return List.of();
            }

            return messagesNode.valueStream()
                    .filter(Objects::nonNull)
                    .map(node -> node.get("value"))
                    .filter(Objects::nonNull)
                    .map(JsonNode::toString)
                    .toList();

        } catch (Exception e) {
            log.error("Message contain doesn't adhere to the JSON template");
            return List.of();
        }
    }
}
