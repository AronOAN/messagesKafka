package cl.messages.service;

import cl.messages.dto.RequestMessageDTO;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;
import java.util.concurrent.CompletableFuture;

@Service
@Log4j2
public class MessageService {
    private static final String TOPIC = "emailss";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonMapper mapper;

    public MessageService(KafkaTemplate<String, String> kafkaTemplate, JsonMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    public CompletableFuture<RecordMetadata> sendMessage(RequestMessageDTO dto) {
        try {
            String json = mapper.writeValueAsString(dto);
            return kafkaTemplate.send(TOPIC, json).thenApply(result -> {
                RecordMetadata meta = result.getRecordMetadata();
                log.info("Kafka confirmado topic={} partition={} offset={}",
                        meta.topic(), meta.partition(), meta.offset());
                return meta;
            });
        } catch (JacksonException e) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("No fue posible serializar el DTO", e));
        }
    }
}
